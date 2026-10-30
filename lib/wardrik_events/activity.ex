defmodule WardrikEvents.Activity do
  @moduledoc """
  A view of what an agent is doing now, as the protocol's `activity`.

      %{
        "doing" => [%{"type" => "wardrik.tool_call.started", "tool" => %{"name" => "github__search_issues", "call" => %{"id" => "call_1"}}, "since" => "..."}],
        "since" => "2026-10-22T09:00:03.000000Z",
        "pending" => 1
      }

    * `"doing"` - every tool call running, oldest first, each with its `"since"`. With no tool
      call running, the last thing the agent started, such as a model call or a wait on a person.
      Empty once a run ends.
    * `"since"` - when `"doing"` last changed.
    * `"pending"` - how many messages wait for a run.

  `WardrikEvents.activity/2` returns the current one.

  ## Options

    * `:since` - the starting `"since"`. Now unless set.
  """

  @behaviour WardrikEvents.View

  @salient ~w(wardrik.run.started wardrik.run.resumed wardrik.run.interrupted wardrik.message.started
              wardrik.message.retrying wardrik.step.started)
  @ended ~w(wardrik.run.completed wardrik.run.failed wardrik.run.cancelled
            wardrik.run.deadline_exceeded)
  @settled ~w(wardrik.tool_call.completed wardrik.tool_call.denied)
  @running ~w(wardrik.tool_call.started wardrik.tool_call.retrying)
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

    case doing(current, type, data, time) do
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

  defp doing(_doing, type, _data, _time) when type in @ended, do: []

  defp doing(doing, type, data, time) when type in @running do
    call = type |> described(data) |> Map.put("since", time)

    if calls?(doing),
      do: Enum.reject(doing, &same_call?(&1, data)) ++ [call],
      else: [call]
  end

  defp doing(doing, type, data, _time) when type in @settled do
    with true <- calls?(doing),
         [_running | _rest] = running <- Enum.reject(doing, &same_call?(&1, data)) do
      running
    else
      _settled -> [described(type, data)]
    end
  end

  defp doing(_doing, type, data, _time) when type in @salient, do: [described(type, data)]
  defp doing(doing, _type, _data, _time), do: doing

  defp calls?([_call | _calls] = doing),
    do: Enum.all?(doing, &(&1["type"] in @running))

  defp calls?([]), do: false

  defp same_call?(running, %{"tool" => %{"call" => %{"id" => id}}}),
    do: running["tool"]["call"]["id"] == id

  defp described(type, %{"tool" => tool} = data),
    do: type |> described(Map.delete(data, "tool")) |> Map.put("tool", tool_ref(tool))

  defp described(type, data), do: data |> Map.take(@described) |> Map.put("type", type)

  defp tool_ref(%{"name" => name, "call" => %{"id" => id}}),
    do: %{"name" => name, "call" => %{"id" => id}}
end
