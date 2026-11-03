defmodule WardrikEvents.Timeline do
  @moduledoc """
  A view of a conversation: each message people sent, and each run that answered, step by step.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "agt_1", WardrikEvents.Timeline)
      WardrikEvents.Timeline.items(timeline)

  To keep a screen current, fold in each new event and redraw what `touched/1` names.

  ## Items

  Each item has an `:id` and a `:kind`:

    * `:user` - a message someone sent. Has `:text`, `:sender` and `:at`.
    * `:run` - the agent's answer. Has `:status`, `:steps` (oldest first), `:model`,
      `:cost_micro_usd`, `:started_at`, `:finished_at`, `:error` and `:interrupts`. `:status` is
      `:running`, `:completed`, `:failed`, `:cancelled` or `:waiting`.

  ## Steps

  Each step has an `:id` and a `:type`:

    * `:thinking` and `:text` - have `:text`, and `:live` while it streams.
    * `:tool` - has `:name`, `:arguments`, `:status`, `:result` and `:error`. `:status` is
      `:running`, `:ok`, `:error` or `:denied`.
    * `:subagent` - a task handed to a delegate. Has `:name`, `:prompt`, `:status`, `:tools`, the
      tool calls it made, `:answer` and `:error`. `:status` is `:running`, `:ok` or `:error`.

  What a delegate says and does folds into its step, never into the run's own answer. A delegate's
  delegates count toward it, and what it cost counts toward the run.
  """

  @behaviour WardrikEvents.View

  alias WardrikEvents.CloudEvent

  defstruct items: %{}, order: [], touched: [], current: nil, delegated: %{}

  @type t() :: %__MODULE__{
          items: %{String.t() => map()},
          order: [String.t()],
          touched: [String.t()] | :reset,
          current: String.t() | nil,
          delegated: %{String.t() => String.t()}
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
  def handle_event(%{"type" => type, "data" => data} = event, timeline) do
    timeline = %{timeline | touched: []}

    case {delegate_of(timeline, event), type} do
      {nil, "wardrik.subagent.started"} -> delegated(timeline, data, event)
      {nil, _type} -> fold(timeline, type, data, event)
      {step_id, _type} -> in_delegate(timeline, step_id, type, data, event)
    end
  end

  def handle_event(_event, timeline), do: %{timeline | touched: []}

  @doc "Returns the items, oldest first."
  @spec items(t()) :: [map()]
  def items(%__MODULE__{items: items, order: order}),
    do: order |> Enum.reverse() |> Enum.map(&Map.fetch!(items, &1))

  @doc "Returns the item `id`, or `nil`."
  @spec item(t(), String.t()) :: map() | nil
  def item(%__MODULE__{items: items}, id), do: Map.get(items, id)

  @doc """
  Returns the ids of the items the last event changed, or `:reset` where it cleared the
  conversation and every item before it is gone.
  """
  @spec touched(t()) :: [String.t()] | :reset
  def touched(%__MODULE__{touched: touched}), do: touched

  @doc "Returns the latest run, or `nil` before any."
  @spec current_run(t()) :: map() | nil
  def current_run(%__MODULE__{current: nil}), do: nil
  def current_run(timeline), do: item(timeline, timeline.current)

  @doc "Returns whether the latest run is going on."
  @spec running?(t()) :: boolean()
  def running?(timeline), do: match?(%{status: :running}, current_run(timeline))

  @doc "Returns the calls the runs wait on a person for, oldest first."
  @spec interrupts(t()) :: [map()]
  def interrupts(timeline),
    do: timeline |> items() |> Enum.flat_map(&Map.get(&1, :interrupts, []))

  defp fold(_t, "wardrik.conversation.cleared", _data, _event),
    do: %__MODULE__{touched: :reset}

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
    t
    |> put(%{
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
    |> Map.put(:current, "run-" <> id)
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

  # A delegate's events are under its span, or a span opened inside it.
  defp delegate_of(t, event),
    do:
      Map.get(t.delegated, CloudEvent.span_id(event)) ||
        Map.get(t.delegated, event["parentspanid"])

  defp delegated(t, data, event) do
    span = CloudEvent.span_id(event)
    id = "agent-" <> span
    t = %{t | delegated: Map.put(t.delegated, span, id)}

    update_run(t, event, fn run ->
      put_step(run, %{
        id: id,
        type: :subagent,
        name: get_in(data, ["agent", "name"]),
        prompt: data["prompt"],
        status: :running,
        tools: 0,
        answer: nil,
        error: nil
      })
    end)
  end

  defp in_delegate(t, step_id, type, data, event) do
    span = CloudEvent.span_id(event)
    t = %{t | delegated: Map.put(t.delegated, span, step_id)}
    own? = "agent-" <> span == step_id

    case {type, own?} do
      {"wardrik.subagent.progress", true} ->
        update_delegate(t, event, step_id, &%{&1 | tools: data["tool_calls"] || &1.tools})

      {"wardrik.subagent.completed", true} ->
        concluded(t, event, step_id, data, &%{&1 | status: :ok, answer: answer(data)})

      {"wardrik.subagent.failed", true} ->
        concluded(
          t,
          event,
          step_id,
          data,
          &%{&1 | status: :error, error: error_text(data["error"])}
        )

      _other ->
        t
    end
  end

  defp answer(%{"message" => message}), do: text(message, "text")
  defp answer(_data), do: nil

  defp concluded(t, event, step_id, data, fun) do
    t
    |> update_delegate(event, step_id, &fun.(%{&1 | tools: data["tool_calls"] || &1.tools}))
    |> update_run(event, &Map.update!(&1, :cost_micro_usd, fn spent -> spent + cost(data) end))
  end

  defp update_delegate(t, event, step_id, fun) do
    update_run(t, event, fn run ->
      case find_step(run, step_id) do
        nil -> run
        step -> put_step(run, fun.(step))
      end
    end)
  end

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

  defp settled(%{type: type} = step) when type in [:thinking, :text], do: %{step | live: false}
  defp settled(step), do: step

  defp answered(run, %{"id" => id} = message, data) do
    run
    |> completed("think-" <> id, :thinking, text(message, "reasoning"))
    |> completed("text-" <> id, :text, text(message, "text"))
    |> Map.update!(:cost_micro_usd, &(&1 + cost(data)))
    |> Map.update!(:error, &(error_text(data["error"]) || &1))
  end

  defp cost(data), do: get_in(data, ["cost", "micro_usd"]) || 0

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
