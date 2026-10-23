defmodule WardrikEvents.ListenerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import WardrikEvents.Test.Events

  alias WardrikEvents.Cursor

  defmodule Told do
    use WardrikEvents.Listener

    @impl true
    def handle(event, listener) do
      send(listener.options[:test], {:handled, event["sequence"], listener.cursor})
      :ok
    end
  end

  defmodule Kept do
    use WardrikEvents.Listener

    @impl true
    def handle(event, listener) do
      saved = listener.options[:saved]

      if Agent.get_and_update(saved, &failing(&1, event["sequence"])) do
        :error
      else
        Agent.update(saved, &%{&1 | cursor: Cursor.encode(listener.cursor)})
        send(listener.options[:test], {:handled, event["sequence"]})
        :ok
      end
    end

    @impl true
    def cursor(listener), do: Agent.get(listener.options[:saved], & &1.cursor)

    defp failing(%{fail: sequence} = saved, sequence), do: {true, %{saved | fail: nil}}
    defp failing(saved, _sequence), do: {false, saved}
  end

  setup :start_events

  test "hands each event over with the cursor past it, then follows", %{events: events} do
    stopped(events, "acme/triage")

    start_supervised!(
      {Told, events: events, resources: ["acme/*"], after: Cursor.encode(%{}), test: self()}
    )

    assert_receive {:handled, "000000000001", %{"acme/triage" => 1}}

    stopped(events, "acme/triage")
    assert_receive {:handled, "000000000002", %{"acme/triage" => 2}}
  end

  test "carries on from the cursor it saved after a handle that failed", %{events: events} do
    for _n <- 1..3, do: stopped(events, "acme/triage")
    {:ok, saved} = Agent.start_link(fn -> %{cursor: Cursor.encode(%{}), fail: "000000000002"} end)

    log =
      capture_log(fn ->
        start_supervised!({Kept, events: events, saved: saved, test: self()})

        assert_receive {:handled, "000000000001"}
        assert_receive {:handled, "000000000002"}
        assert_receive {:handled, "000000000003"}
      end)

    assert log =~ "MatchError"
    refute_received {:handled, _sequence}
  end
end
