defmodule WardrikEvents.RetentionTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import WardrikEvents.Test.Events

  alias WardrikEvents.Cursor
  alias WardrikEvents.Retention

  setup :start_events

  setup %{events: events} do
    stopped(events, "agt_1", ~U[2000-01-01 00:00:00Z])
    stopped(events, "agt_1", ~U[2000-01-02 00:00:00Z])
    stopped(events, "agt_1")
    :ok
  end

  defp dropped(events, cursor) do
    {:ok, %{events: [%{"type" => "wardrik.events.dropped"} = dropped | _rest]}} =
      WardrikEvents.page(events, ["agt_1"], after: Cursor.encode(cursor))

    dropped["data"]
  end

  test "drops the events written before the cutoff, and says so to a reader after them", %{
    events: events
  } do
    :ok = WardrikEvents.trim(events, ~U[2000-01-03 00:00:00Z])

    assert sequences(events, ["agt_1"]) == ["wardrik.events.dropped", "000000000003"]
    assert dropped(events, %{}) == %{"after" => 0, "oldest" => 3}
    assert dropped(events, %{{"agt_1", nil} => 1}) == %{"after" => 1, "oldest" => 3}

    assert sequences(events, ["agt_1"], after: Cursor.encode(%{{"agt_1", nil} => 2})) ==
             ["000000000003"]
  end

  test "drops what's already dropped once", %{events: events} do
    :ok = WardrikEvents.trim(events, ~U[2000-01-02 00:00:00Z])
    :ok = WardrikEvents.trim(events, ~U[2000-01-02 00:00:00Z])
    :ok = WardrikEvents.trim(events, ~U[2000-01-03 00:00:00Z])

    assert dropped(events, %{}) == %{"after" => 0, "oldest" => 3}
  end

  test "carries on numbering after every event is dropped and the events restart", %{
    events: events,
    store: store
  } do
    :ok = WardrikEvents.trim(events, DateTime.add(DateTime.utc_now(), 1, :day))
    assert {:ok, %{{"agt_1", nil} => 3}} = Cursor.decode(WardrikEvents.head(events, ["agt_1"]))

    stop_supervised!(WardrikEvents)
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    trimmed(events)
    stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) == ["wardrik.events.dropped", "000000000004"]
  end

  test "trims as it starts and every hour after, keeping the days it's given", %{
    events: events
  } do
    {Retention, retention, :worker, _modules} =
      events |> Supervisor.which_children() |> List.keyfind(Retention, 0)

    send(retention, :trim)
    trimmed(events)

    assert sequences(events, ["agt_1"]) == ["wardrik.events.dropped", "000000000003"]
  end

  test "logs a trim it couldn't make", %{events: events} do
    expect(WardrikStore, :put, fn _store, _key, _value, _opts -> {:error, :down} end)

    assert capture_log(fn -> WardrikEvents.trim(events, ~U[2000-01-03 00:00:00Z]) end) =~
             ~s(Couldn't trim {"agt_1", nil}'s events: :down)
  end

  test "keeps every event with infinite retention", %{store: store} do
    start_supervised!({WardrikEvents, name: :infinite, store: store, retention: :infinity},
      id: :infinite
    )

    refute :infinite |> Supervisor.which_children() |> List.keyfind(Retention, 0)
  end
end
