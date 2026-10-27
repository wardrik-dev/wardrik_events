defmodule WardrikEvents.Encoder do
  @moduledoc """
  Encodes an agent's `WardrikCore.Event`s as the CloudEvents of `WardrikProtocol`.
  """

  alias WardrikCore.Authorization
  alias WardrikCore.Event
  alias WardrikCore.Span
  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Encoder.Value

  @envelope [:agent_id, :span]

  @doc "Encodes `event` as the CloudEvents it is, for the deployment named `deployment`."
  @spec encode(Event.t(), String.t()) :: [CloudEvent.t()]
  def encode(%Event{metadata: %{agent_id: agent_id}} = event, deployment) do
    opts = [
      deployment: deployment,
      agent_id: agent_id,
      time: event.time,
      span: event.metadata[:span]
    ]

    event
    |> described()
    |> Enum.map(fn {type, subject, data} -> CloudEvent.new(type, subject, data, opts) end)
  end

  def encode(%Event{}, _deployment), do: []

  defp described(%Event{name: [:wardrik, noun, verb], metadata: metadata}),
    do: [
      {"#{noun}.#{verb}", subject(noun, metadata),
       metadata |> Map.drop(@envelope) |> Value.json()}
    ]

  defp described(%Event{name: [:wardrik_core | name]} = event), do: core(name, event)
  defp described(_event), do: []

  defp core([:agent, :start], %{metadata: metadata}) do
    [
      {"agent.started", "agents/" <> metadata.agent_id,
       %{
         "agent" => %{"id" => metadata.agent_id},
         "conversation" => %{"id" => metadata.agent_id},
         "node" => Atom.to_string(node()),
         "status" => agent_status(metadata.state)
       }}
    ]
  end

  defp core([:agent, :stop], %{metadata: metadata}),
    do: [
      {"agent.stopped", "agents/" <> metadata.agent_id,
       %{"agent" => %{"id" => metadata.agent_id}}}
    ]

  defp core([:message, :queued], %{metadata: %{message: message}}) do
    data =
      Value.put(
        %{"message" => Value.message(message)},
        "sender",
        Value.principal(message.metadata[:principal])
      )

    [{"message.created", "messages/" <> message.metadata[:id], data}]
  end

  defp core([:turn, :start], %{metadata: %{kind: :run} = metadata}),
    do: [run("run.started", metadata, %{}, Value.put(%{}, "message_ids", metadata[:message_ids]))]

  defp core([:turn, :start], %{metadata: %{kind: :resume} = metadata}),
    do: [run("run.resumed", metadata, %{})]

  defp core([:turn, :stop], event), do: [stopped(event.metadata.outcome, event)]
  defp core([:message, :delta], %{metadata: metadata}), do: [delta(metadata)]

  defp core([:message, :retrying], %{metadata: metadata}),
    do: [about_message("message.retrying", metadata.span, retried(metadata))]

  defp core([:tool_call, :retrying], %{metadata: %{call: call} = metadata}),
    do: [
      {"tool_call.retrying", "tool_calls/" <> call.id,
       Map.put(retried(metadata), "tool", tool(call))}
    ]

  defp core([:step, :start], %{metadata: metadata}), do: step_started(metadata.step, metadata)
  defp core([:step, :stop], event), do: step_stopped(event.metadata.step, event)
  defp core([:step, :exception], event), do: step_failed(event.metadata.step, event)

  defp core([:tool_call, :exit], %{metadata: %{call: call, reason: reason}}),
    do: [
      {"tool_call.completed", "tool_calls/" <> call.id,
       error_status(%{"tool" => tool(call), "replayed" => false}, exited(reason))}
    ]

  defp core([:subagent, :started], %{metadata: metadata}),
    do: [subagent("subagent.started", metadata, %{"prompt" => metadata.prompt})]

  defp core([:subagent, :completed], %{metadata: metadata}),
    do: [
      subagent("subagent.completed", metadata, %{
        "message" => Value.message(metadata.message),
        "duration_ms" => metadata.duration_ms
      })
    ]

  defp core([:subagent, :failed], %{metadata: metadata}),
    do: [
      subagent("subagent.failed", metadata, %{
        "error" => Value.error(metadata.error),
        "duration_ms" => metadata.duration_ms
      })
    ]

  defp core(_internal, _event), do: []

  defp subagent(type, metadata, data),
    do:
      {type, "agents/" <> metadata.agent_id,
       Map.put(data, "agent", %{"name" => metadata.agent.name})}

  defp stopped(:ok, %{metadata: metadata}),
    do:
      run("run.completed", metadata, %{
        "messages" => Enum.map(metadata.messages, &Value.message/1)
      })

  defp stopped(:park, %{metadata: metadata}),
    do: run("run.interrupted", metadata, %{"interrupts" => interrupts(metadata.reason)})

  defp stopped(:interrupted, event) do
    run("run.cancelled", event.metadata, %{
      "dropped" => event.metadata.dropped,
      "kept" => event.metadata.kept,
      "resolution" => "stopped",
      "duration_ms" => milliseconds(event.measurements)
    })
  end

  defp stopped(:timeout, %{metadata: metadata}),
    do: run("run.deadline_exceeded", metadata, %{"message_count" => metadata.kept})

  defp stopped(:crash, %{metadata: metadata}),
    do: run("run.failed", metadata, %{"error" => Value.error(:crash)})

  defp stopped(:error, %{metadata: metadata}),
    do: run("run.failed", metadata, %{"error" => Value.error(metadata.reason)})

  defp run(type, %{span: %Span{trace: trace}}, data, run \\ %{}),
    do: {type, "runs/" <> trace, Map.put(data, "run", Map.put(run, "id", trace))}

  defp retried(metadata) do
    %{
      "attempt" => metadata.attempt,
      "attempts" => metadata.attempts,
      "wait_ms" => metadata.wait_ms,
      "error" => Value.error(metadata.reason)
    }
  end

  defp delta(%{kind: kind, span: span, text: text}),
    do: about_message(delta_type(kind), span, %{"delta" => text})

  defp delta_type(:content), do: "message.delta"
  defp delta_type(:thinking), do: "reasoning.delta"

  defp step_started(:generate, %{span: span, model: model}) do
    {provider, name} = model_parts(model)

    data =
      %{"request" => %{"model" => name}, "decided" => default()}
      |> Value.put("provider", provider && %{"name" => provider})

    [about_message("message.started", span, data)]
  end

  defp step_started({:tool_call, call}, _metadata),
    do: [
      {"tool_call.started", "tool_calls/" <> call.id,
       %{"tool" => tool(call), "decided" => default()}}
    ]

  defp step_started({:step, name}, %{span: span}),
    do: [{"step.started", "steps/" <> span.id, %{"step" => %{"name" => name}}}]

  defp step_started(:turn, _metadata), do: []

  defp step_stopped(:generate, %{metadata: metadata, measurements: measurements}) do
    id = message_id(metadata.span)

    data =
      %{"message" => answer(metadata, id), "duration_ms" => milliseconds(measurements)}
      |> status(metadata)
      |> Value.put("response", response(metadata[:response]))
      |> Value.put("usage", Value.usage(measurements))
      |> priced(metadata, measurements)

    [{"message.completed", "messages/" <> id, data}]
  end

  defp step_stopped({:tool_call, _call}, %{metadata: %{status: :park}}), do: []

  defp step_stopped({:tool_call, call}, %{metadata: metadata, measurements: measurements}) do
    data =
      %{"tool" => tool(call, metadata[:response]), "replayed" => false}
      |> tool_status(metadata[:response], metadata)
      |> Value.put("cost", Value.cost(measurements, "tool"))

    [{"tool_call.completed", "tool_calls/" <> call.id, data}]
  end

  defp step_stopped({:step, _name}, %{metadata: metadata, measurements: measurements}),
    do: [
      {"step.completed", "steps/" <> metadata.span.id,
       status(%{"duration_ms" => milliseconds(measurements)}, metadata)}
    ]

  defp step_stopped(:turn, _event), do: []

  defp step_failed(:turn, _event), do: []

  defp step_failed(step, %{metadata: metadata} = event),
    do:
      step_stopped(step, %{
        event
        | metadata: Map.merge(metadata, %{status: :error, reason: metadata.reason})
      })

  defp interrupts({:tool_calls, results}),
    do: for({call, {:park, reason}} <- results, do: interrupt(reason, call))

  defp interrupts(reason), do: [interrupt(reason, nil)]

  defp interrupt(reason, call) do
    {kind, message} = interruption(reason)

    %{"id" => (call && call.id) || "model", "reason" => kind, "message" => message}
    |> Value.put("tool_call_id", call && call.id)
    |> Value.put("name", call && call.name)
    |> Value.put("labels", labels(reason))
  end

  defp labels(%{labels: [_ | _] = labels}), do: labels
  defp labels(_reason), do: nil

  defp interruption(%{interrupt: kind, message: message}), do: {kind, message}

  defp interruption(%Authorization{credential: credential, url: url}),
    do: {"authorization", "Sign in to #{credential} at #{url}."}

  defp interruption(:in_doubt),
    do:
      {"in_doubt",
       "This call may have run before a restart. Approve it to run it again, or deny it."}

  defp interruption(reason), do: {"tool_approval", inspect(reason)}

  defp answer(%{response: %ReqLLM.Response{message: %ReqLLM.Message{} = message}}, id),
    do: Value.message(message, id)

  defp answer(_unanswered, id), do: %{"id" => id, "parts" => []}

  defp response(%ReqLLM.Response{} = response) do
    %{"model" => response.model}
    |> Value.put("id", response.id)
    |> Value.put(
      "finish_reasons",
      response.finish_reason && [Atom.to_string(response.finish_reason)]
    )
  end

  defp response(_none), do: nil

  defp tool(call, response \\ nil),
    do: %{"name" => call.name, "call" => tool_call(call, response)}

  defp tool_call(call, response) do
    %{"id" => call.id}
    |> Value.capped("arguments", call.arguments)
    |> result(response)
  end

  defp priced(data, %{response: %ReqLLM.Response{}}, measurements),
    do: Map.put(data, "cost", Value.cost(measurements, "catalog"))

  defp priced(data, _unanswered, _measurements), do: data

  defp result(tool_call, {:ok, %ReqLLM.ToolResult{output: output}}),
    do: Value.capped(tool_call, "result", output)

  defp result(tool_call, {:ok, answer}), do: Value.capped(tool_call, "result", answer)
  defp result(tool_call, _no_answer), do: tool_call

  defp tool_status(data, {:ok, _answer}, _metadata), do: Map.put(data, "status", "ok")
  defp tool_status(data, {:error, reason}, _metadata), do: error_status(data, reason)
  defp tool_status(data, _response, metadata), do: error_status(data, metadata[:reason])

  defp status(data, %{status: :ok}), do: Map.put(data, "status", "ok")
  defp status(data, metadata), do: error_status(data, metadata[:reason])

  defp error_status(data, reason),
    do: data |> Map.put("status", "error") |> Map.put("error", Value.error(reason))

  defp exited(:killed), do: "The call was stopped before it finished."
  defp exited(reason), do: {:exit, reason}

  defp agent_status(:waiting), do: "parked"
  defp agent_status(:idle), do: "idle"
  defp agent_status(_running_or_due), do: "running"

  defp subject(:tool_call, %{tool: %{call: %{id: id}}}), do: "tool_calls/" <> id
  defp subject(:interrupt, %{interrupt: %{id: id}}), do: "interrupts/" <> id
  defp subject(_noun, %{agent_id: agent_id}), do: "agents/" <> agent_id

  defp about_message(type, span, data) do
    id = message_id(span)
    {type, "messages/" <> id, Map.put(data, "message", %{"id" => id})}
  end

  defp message_id(%Span{id: id}), do: "msg_" <> id

  defp model_parts(model) do
    case String.split(model, ":", parts: 2) do
      [provider, name] -> {provider, name}
      [name] -> {nil, name}
    end
  end

  defp default, do: %{"by" => "default"}

  defp milliseconds(%{duration: duration}),
    do: System.convert_time_unit(duration, :native, :millisecond)
end
