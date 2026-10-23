defmodule WardrikEvents.Runs do
  @moduledoc """
  What an agent has done: each run, with who started it, how long it took, the model, its tool
  calls, the tokens it read and wrote and what it cost, and the totals across them.

      {runs, _cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Runs)
      WardrikEvents.Runs.totals(runs)
  """

  @behaviour WardrikEvents.View

  alias WardrikEvents.CloudEvent

  defstruct runs: %{}, order: [], senders: %{}

  @type run() :: %{
          id: String.t(),
          status: :running | :completed | :failed | :cancelled | :waiting,
          started_at: String.t(),
          finished_at: String.t() | nil,
          source: String.t() | nil,
          model: String.t() | nil,
          tool_calls: non_neg_integer(),
          tool_errors: non_neg_integer(),
          input_tokens: non_neg_integer(),
          output_tokens: non_neg_integer(),
          cost_micro_usd: non_neg_integer(),
          error: String.t() | nil
        }

  @type t() :: %__MODULE__{runs: %{String.t() => run()}, order: [String.t()], senders: map()}

  @ended %{
    "wardrik.run.completed" => :completed,
    "wardrik.run.deadline_exceeded" => :completed,
    "wardrik.run.cancelled" => :cancelled,
    "wardrik.run.failed" => :failed
  }

  @summed [:tool_calls, :input_tokens, :output_tokens, :cost_micro_usd]

  @impl WardrikEvents.View
  def init(_opts), do: %__MODULE__{}

  @impl WardrikEvents.View
  def handle_event(%{"type" => "wardrik.message.created", "data" => data}, runs) do
    sender = get_in(data, ["sender", "name"]) || get_in(data, ["sender", "id"])
    %{runs | senders: Map.put(runs.senders, data["message"]["id"], sender)}
  end

  def handle_event(%{"type" => "wardrik.run.started", "data" => data} = event, runs) do
    id = data["run"]["id"]
    run = started(id, event, source(runs, data["run"]))
    %{runs | runs: Map.put(runs.runs, id, run), order: [id | runs.order]}
  end

  def handle_event(%{"type" => type, "data" => data} = event, runs) do
    case Map.fetch(runs.runs, CloudEvent.trace_id(event) || "") do
      {:ok, run} -> %{runs | runs: Map.put(runs.runs, run.id, step(run, type, data, event))}
      :error -> runs
    end
  end

  def handle_event(_event, runs), do: runs

  @doc "Returns the runs, newest first."
  @spec runs(t()) :: [run()]
  def runs(%__MODULE__{runs: runs, order: order}), do: Enum.map(order, &Map.fetch!(runs, &1))

  @doc "Returns the totals across every run, and when the last one started."
  @spec totals(t()) :: map()
  def totals(state) do
    runs = runs(state)

    sums = for key <- @summed, into: %{}, do: {key, Enum.sum_by(runs, &Map.fetch!(&1, key))}

    Map.merge(sums, %{
      runs: length(runs),
      failed: Enum.count(runs, &(&1.status == :failed)),
      last_at: last_at(runs)
    })
  end

  @doc "Returns how long `run` took in milliseconds, or nil while it goes on."
  @spec duration_ms(run()) :: non_neg_integer() | nil
  def duration_ms(%{started_at: started, finished_at: finished}) when is_binary(finished) do
    {:ok, from, _offset} = DateTime.from_iso8601(started)
    {:ok, to, _offset} = DateTime.from_iso8601(finished)
    DateTime.diff(to, from, :millisecond)
  end

  def duration_ms(_run), do: nil

  defp last_at([newest | _older]), do: newest.started_at
  defp last_at([]), do: nil

  defp started(id, event, source) do
    %{
      id: id,
      status: :running,
      started_at: event["time"],
      finished_at: nil,
      source: source,
      model: nil,
      tool_calls: 0,
      tool_errors: 0,
      input_tokens: 0,
      output_tokens: 0,
      cost_micro_usd: 0,
      error: nil
    }
  end

  defp source(runs, run),
    do: run |> Map.get("message_ids", []) |> Enum.find_value(&runs.senders[&1])

  defp step(run, "wardrik.message.started", data, _event),
    do: %{run | model: get_in(data, ["request", "model"]) || run.model}

  defp step(run, "wardrik.message.completed", data, _event) do
    %{
      run
      | input_tokens: run.input_tokens + (get_in(data, ["usage", "input_tokens"]) || 0),
        output_tokens: run.output_tokens + (get_in(data, ["usage", "output_tokens"]) || 0),
        cost_micro_usd: run.cost_micro_usd + (get_in(data, ["cost", "micro_usd"]) || 0)
    }
  end

  defp step(run, "wardrik.tool_call.started", _data, _event),
    do: %{run | tool_calls: run.tool_calls + 1}

  defp step(run, "wardrik.tool_call.completed", %{"status" => "error"}, _event),
    do: %{run | tool_errors: run.tool_errors + 1}

  defp step(run, "wardrik.tool_call.denied", _data, _event),
    do: %{run | tool_errors: run.tool_errors + 1}

  defp step(run, "wardrik.run.interrupted", _data, _event), do: %{run | status: :waiting}
  defp step(run, "wardrik.run.resumed", _data, _event), do: %{run | status: :running}

  defp step(run, type, data, event) when is_map_key(@ended, type) do
    %{
      run
      | status: Map.fetch!(@ended, type),
        finished_at: event["time"],
        error: get_in(data, ["error", "message"])
    }
  end

  defp step(run, _type, _data, _event), do: run
end
