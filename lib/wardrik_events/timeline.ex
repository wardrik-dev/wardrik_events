defmodule WardrikEvents.Timeline do
  @moduledoc """
  An agent's conversation as a person reads it: the messages people sent, and the run that answered
  them, step by step.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)
      WardrikEvents.Timeline.items(timeline)

  Items are maps with an `:id` and a `:kind`:

    * `:user`: a message someone sent, with `:text`, `:sender` and `:at`
    * `:run`: the agent's answer, with `:status` (`:running`, `:completed`, `:failed`,
      `:cancelled` or `:waiting`), `:steps` oldest first, `:model`, `:cost_micro_usd`,
      `:started_at`, `:finished_at`, `:error` and `:interrupts`

  Steps are `:thinking` and `:text`, with `:text` and `:live` while it streams, and `:tool`, with
  `:name`, `:arguments`, `:status` (`:running`, `:ok`, `:error` or `:denied`), `:result` and
  `:error`. `touched/1` names the items the last event changed.
  """

  @behaviour WardrikEvents.View

  alias WardrikEvents.CloudEvent

  defstruct items: %{}, order: [], touched: []

  @type t() :: %__MODULE__{
          items: %{String.t() => map()},
          order: [String.t()],
          touched: [String.t()]
        }

  @ended %{
    "wardrik.run.completed" => :completed,
    "wardrik.run.deadline_exceeded" => :completed,
    "wardrik.run.cancelled" => :cancelled,
    "wardrik.run.failed" => :failed
  }

  @error_limit 300

  @impl WardrikEvents.View
  def init(_opts), do: %__MODULE__{}

  @impl WardrikEvents.View
  def handle_event(%{"type" => type, "data" => data} = event, timeline),
    do: fold(%{timeline | touched: []}, type, data, event)

  def handle_event(_event, timeline), do: %{timeline | touched: []}

  @doc "Returns the items, oldest first."
  @spec items(t()) :: [map()]
  def items(%__MODULE__{items: items, order: order}),
    do: order |> Enum.reverse() |> Enum.map(&Map.fetch!(items, &1))

  @doc "Returns the ids of the items the last event changed."
  @spec touched(t()) :: [String.t()]
  def touched(%__MODULE__{touched: touched}), do: touched

  defp fold(
         t,
         "wardrik.message.created",
         %{"message" => %{"role" => "user"} = message} = data,
         event
       ) do
    put(t, %{
      id: "msg-" <> message["id"],
      kind: :user,
      text: text(message, "text"),
      sender: get_in(data, ["sender", "name"]),
      at: event["time"]
    })
  end

  defp fold(t, "wardrik.run.started", %{"run" => %{"id" => id}}, event) do
    put(t, %{
      id: "run-" <> id,
      kind: :run,
      status: :running,
      steps: [],
      model: nil,
      cost_micro_usd: 0,
      started_at: event["time"],
      finished_at: nil,
      error: nil,
      interrupts: []
    })
  end

  defp fold(t, "wardrik.run.resumed", _data, event),
    do: update_run(t, event, &%{&1 | status: :running, interrupts: []})

  defp fold(t, "wardrik.run.interrupted", %{"interrupts" => interrupts}, event),
    do: update_run(t, event, &%{&1 | status: :waiting, interrupts: interrupts})

  defp fold(t, type, data, event) when is_map_key(@ended, type),
    do: update_run(t, event, &finished(&1, Map.fetch!(@ended, type), data, event))

  defp fold(t, "wardrik.interrupt.resolved", %{"interrupt" => %{"id" => id}}, _event) do
    case Enum.find(t.items, fn {_id, item} -> waiting_on?(item, id) end) do
      nil -> t
      {_id, run} -> put(t, %{run | interrupts: Enum.reject(run.interrupts, &(&1["id"] == id))})
    end
  end

  defp fold(t, "wardrik.message.started", data, event),
    do: update_run(t, event, &%{&1 | model: get_in(data, ["request", "model"]) || &1.model})

  defp fold(t, "wardrik.reasoning.delta", %{"message" => %{"id" => id}, "delta" => delta}, event),
    do: update_run(t, event, &streamed(&1, "think-" <> id, :thinking, delta))

  defp fold(t, "wardrik.message.delta", %{"message" => %{"id" => id}, "delta" => delta}, event),
    do: update_run(t, event, &streamed(&1, "text-" <> id, :text, delta))

  defp fold(t, "wardrik.message.completed", %{"message" => message} = data, event),
    do: update_run(t, event, &answered(&1, message, data))

  defp fold(t, "wardrik.tool_call.started", %{"tool" => tool}, event),
    do: update_run(t, event, &put_step(&1, tool_step(tool)))

  defp fold(t, "wardrik.tool_call.completed", %{"tool" => tool} = data, event) do
    update_tool(t, event, tool, fn step ->
      %{
        step
        | status: if(data["status"] == "error", do: :error, else: :ok),
          result: get_in(tool, ["call", "result"]),
          error: error_text(data["error"])
      }
    end)
  end

  defp fold(t, "wardrik.tool_call.denied", %{"tool" => tool} = data, event),
    do: update_tool(t, event, tool, &%{&1 | status: :denied, error: data["reason"]})

  defp fold(t, _type, _data, _event), do: t

  defp finished(run, status, data, event) do
    %{
      run
      | status: status,
        finished_at: event["time"],
        interrupts: [],
        error: error_text(data["error"]) || run.error,
        steps: Enum.map(run.steps, &settled/1)
    }
  end

  defp settled(%{type: :tool} = step), do: step
  defp settled(step), do: %{step | live: false}

  defp answered(run, %{"id" => id} = message, data) do
    run
    |> completed("think-" <> id, :thinking, text(message, "reasoning"))
    |> completed("text-" <> id, :text, text(message, "text"))
    |> Map.update!(:cost_micro_usd, &(&1 + (get_in(data, ["cost", "micro_usd"]) || 0)))
    |> Map.update!(:error, &(error_text(data["error"]) || &1))
  end

  defp streamed(run, id, type, delta) do
    step = find_step(run, id) || %{id: id, type: type, text: "", live: true}
    put_step(run, %{step | text: step.text <> delta, live: true})
  end

  defp completed(run, id, type, text) do
    case {find_step(run, id), text} do
      {nil, ""} -> run
      {nil, text} -> put_step(run, %{id: id, type: type, text: text, live: false})
      {step, ""} -> put_step(run, %{step | live: false})
      {step, text} -> put_step(run, %{step | text: text, live: false})
    end
  end

  defp update_tool(t, event, tool, fun) do
    update_run(t, event, fn run ->
      step = find_step(run, "tool-" <> get_in(tool, ["call", "id"])) || tool_step(tool)
      put_step(run, fun.(step))
    end)
  end

  defp tool_step(tool) do
    %{
      id: "tool-" <> get_in(tool, ["call", "id"]),
      type: :tool,
      name: tool["name"],
      arguments: get_in(tool, ["call", "arguments"]),
      status: :running,
      result: nil,
      error: nil
    }
  end

  defp update_run(t, event, fun) do
    case Map.get(t.items, "run-" <> (CloudEvent.trace_id(event) || "")) do
      %{kind: :run} = run -> put(t, fun.(run))
      nil -> t
    end
  end

  defp waiting_on?(item, id), do: Enum.any?(Map.get(item, :interrupts, []), &(&1["id"] == id))

  defp find_step(run, id), do: Enum.find(run.steps, &(&1.id == id))

  defp put_step(run, %{id: id} = step) do
    case Enum.find_index(run.steps, &(&1.id == id)) do
      nil -> %{run | steps: run.steps ++ [step]}
      index -> %{run | steps: List.replace_at(run.steps, index, step)}
    end
  end

  defp put(%__MODULE__{items: items, order: order} = t, %{id: id} = item) do
    order = if Map.has_key?(items, id), do: order, else: [id | order]
    %{t | items: Map.put(items, id, item), order: order, touched: [id]}
  end

  defp text(%{"parts" => parts}, type) do
    parts
    |> Enum.filter(&match?(%{"type" => ^type, "content" => content} when is_binary(content), &1))
    |> Enum.map_join("\n\n", & &1["content"])
  end

  defp text(_message, _type), do: ""

  defp error_text(nil), do: nil

  defp error_text(error) do
    message = error["message"] || error["type"] || "Something went wrong."

    if String.length(message) > @error_limit,
      do: String.slice(message, 0, @error_limit) <> "…",
      else: message
  end
end
