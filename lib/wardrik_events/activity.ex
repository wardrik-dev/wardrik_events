defmodule WardrikEvents.Activity do
  @moduledoc """
  What an agent is doing now, since when, and how many messages wait behind it, as the protocol's
  `activity`.

  It's every tool call running, oldest first, or else the one thing the last event that says so
  set. A run ending makes it idle, with nothing in `doing`.
  """

  @behaviour WardrikEvents.View

  @salient ~w(wardrik.run.started wardrik.run.resumed wardrik.run.interrupted wardrik.message.started
              wardrik.message.retrying wardrik.step.started)
  @ended ~w(wardrik.run.completed wardrik.run.failed wardrik.run.cancelled
            wardrik.run.deadline_exceeded)
  @settled ~w(wardrik.tool_call.completed wardrik.tool_call.denied)
  @described ~w(message step interrupts attempt attempts wait_ms)

  @impl WardrikEvents.View
  def init(opts) do
    since = Keyword.get_lazy(opts, :since, &DateTime.utc_now/0)
    %{"doing" => [], "since" => DateTime.to_iso8601(since), "pending" => 0}
  end

  @impl WardrikEvents.View
  def handle_event(%{"type" => type, "data" => data, "time" => time}, activity) do
    activity = %{activity | "pending" => max(activity["pending"] + queued(type, data), 0)}
    current = activity["doing"]

    case doing(current, type, data) do
      ^current -> activity
      doing -> %{activity | "doing" => doing, "since" => time}
    end
  end

  def handle_event(_event, activity), do: activity

  defp queued("wardrik.message.created", _data), do: 1

  defp queued("wardrik.run.started", %{"run" => run}),
    do: -length(Map.get(run, "message_ids", []))

  defp queued("wardrik.run.cancelled", %{"dropped" => dropped}), do: -dropped
  defp queued(_type, _data), do: 0

  defp doing(_doing, type, _data) when type in @ended, do: []

  defp doing(doing, "wardrik.tool_call.started" = type, data) do
    if calls?(doing),
      do: doing ++ [described(type, data)],
      else: [described(type, data)]
  end

  defp doing(doing, type, data) when type in @settled do
    with true <- calls?(doing),
         [_running | _rest] = running <- Enum.reject(doing, &same_call?(&1, data)) do
      running
    else
      _settled -> [described(type, data)]
    end
  end

  defp doing(_doing, type, data) when type in @salient, do: [described(type, data)]
  defp doing(doing, _type, _data), do: doing

  defp calls?([_call | _calls] = doing),
    do: Enum.all?(doing, &(&1["type"] == "wardrik.tool_call.started"))

  defp calls?([]), do: false

  defp same_call?(running, %{"tool" => %{"call" => %{"id" => id}}}),
    do: running["tool"]["call"]["id"] == id

  defp described(type, %{"tool" => tool} = data),
    do: type |> described(Map.delete(data, "tool")) |> Map.put("tool", tool_ref(tool))

  defp described(type, data), do: data |> Map.take(@described) |> Map.put("type", type)

  defp tool_ref(%{"name" => name, "call" => %{"id" => id}}),
    do: %{"name" => name, "call" => %{"id" => id}}
end
