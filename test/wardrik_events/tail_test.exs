defmodule WardrikEvents.TailTest do
  use ExUnit.Case, async: false

  import WardrikEvents.Test.Events

  alias WardrikEvents.Cursor

  setup :start_events

  defp sequence({event, _cursor}), do: event["sequence"] || event["type"]

  defp following(stream, events, sequence) do
    Stream.each(stream, fn
      {%{"sequence" => ^sequence}, _cursor} -> stopped(events, "agt_1")
      _other -> :ok
    end)
  end

  defp written(store, sequence) do
    number = sequence |> Integer.to_string() |> String.pad_leading(12, "0")
    event = %{"type" => "wardrik.agent.stopped", "sequence" => number}
    {:ok, _vsn} = WardrikStore.put(store, "events/agt_1/" <> number, event)
    send(self(), {WardrikEvents, {"agt_1", nil}, event})
  end

  describe "subscribing" do
    test "sends each event to the processes whose resources cover its log", %{events: events} do
      :ok = WardrikEvents.subscribe(events, ["agt_1", {"agt_2", "C1"}])

      for log <- ["agt_1", {"agt_1", "C1"}, "agt_10", "agt_2", {"agt_2", "C1"}, {"agt_2", "C2"}],
          do: stopped(events, log)

      assert_received {WardrikEvents, {"agt_1", nil}, _event}
      assert_received {WardrikEvents, {"agt_1", "C1"}, _event}
      assert_received {WardrikEvents, {"agt_2", "C1"}, _event}
      refute_received {WardrikEvents, {"agt_1", nil}, _event}
      refute_received {WardrikEvents, _log, _event}
    end

    test "sends a conversation's subscriber none of the agent's other logs", %{events: events} do
      :ok = WardrikEvents.subscribe(events, [{"agt_1", "C1"}])
      for log <- ["agt_1", {"agt_1", "C2"}, {"agt_1", "C1"}], do: stopped(events, log)

      assert_received {WardrikEvents, {"agt_1", "C1"}, _event}
      refute_received {WardrikEvents, _log, _event}
    end

    test "sends every log to a subscriber to :all", %{events: events} do
      :ok = WardrikEvents.subscribe(events, :all)
      for log <- ["agt_1", {"agt_2", "C1"}], do: stopped(events, log)

      assert_received {WardrikEvents, {"agt_1", nil}, _event}
      assert_received {WardrikEvents, {"agt_2", "C1"}, _event}

      :ok = WardrikEvents.unsubscribe(events, :all)
      stopped(events, "agt_1")

      refute_received {WardrikEvents, _log, _event}
    end

    test "stops sending once unsubscribed", %{events: events} do
      :ok = WardrikEvents.subscribe(events, ["agt_1", {"agt_1", nil}, {"agt_1", "C1"}])
      :ok = WardrikEvents.unsubscribe(events, ["agt_1"])
      stopped(events, {"agt_1", "C2"})
      stopped(events, {"agt_1", "C1"})

      refute_received {WardrikEvents, {"agt_1", "C2"}, _event}
      assert_received {WardrikEvents, {"agt_1", "C1"}, _event}

      :ok = WardrikEvents.unsubscribe(events, [{"agt_1", nil}, {"agt_1", "C1"}])
      stopped(events, {"agt_1", "C1"})

      refute_received {WardrikEvents, _log, _event}
    end
  end

  describe "streaming" do
    test "reads on from a cursor, then follows what's written", %{events: events} do
      stopped(events, "agt_1")
      stopped(events, "agt_1")
      cursor = Cursor.encode(%{{"agt_1", nil} => 1})

      {:ok, stream} = WardrikEvents.stream(events, ["agt_1"], after: cursor)

      taken = stream |> following(events, "000000000002") |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["000000000002", "000000000003"]
      assert {_event, %{{"agt_1", nil} => 3}} = List.last(taken)
      refute_received {WardrikEvents, _log, _event}
    end

    test "starts at what's written next without a cursor", %{events: events} do
      stopped(events, "agt_1")
      send(self(), {WardrikEvents, {"agt_1", nil}, %{"type" => "wardrik.message.delta"}})

      {:ok, stream} = WardrikEvents.stream(events, ["agt_1"])

      taken =
        stream
        |> Stream.each(fn {event, _cursor} ->
          if event["type"] == "wardrik.message.delta", do: stopped(events, {"agt_1", "C1"})
        end)
        |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["wardrik.message.delta", "000000000001"]

      assert [{_delta, head}, {%{"conversation" => "C1"}, _cursor}] =
               taken

      assert %{{"agt_1", nil} => 1} = head
    end

    test "follows one conversation's log, and none of the agent's others", %{events: events} do
      for log <- ["agt_1", {"agt_1", "C2"}, {"agt_1", "C1"}], do: stopped(events, log)
      {:ok, stream} = WardrikEvents.stream(events, [{"agt_1", "C1"}], after: Cursor.encode(%{}))

      taken =
        stream
        |> Stream.each(fn _taken ->
          stopped(events, {"agt_1", "C2"})
          stopped(events, {"agt_1", "C1"})
        end)
        |> Enum.take(2)

      assert [{first, _cursor}, {second, cursor}] = taken
      assert %{"conversation" => "C1", "sequence" => "000000000001"} = first
      assert %{"conversation" => "C1", "sequence" => "000000000002"} = second
      assert cursor == %{{"agt_1", "C1"} => 2}
    end

    test "takes each kept event once", %{events: events} do
      stopped(events, "agt_1")
      {:ok, %{events: [first]}} = WardrikEvents.page(events, ["agt_1"])
      send(self(), {WardrikEvents, {"agt_1", nil}, first})

      {:ok, stream} = WardrikEvents.stream(events, ["agt_1"], after: Cursor.encode(%{}))

      taken = stream |> following(events, "000000000001") |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["000000000001", "000000000002"]
    end

    test "waits for an event written ahead of one still being written", %{
      events: events,
      store: store
    } do
      store = WardrikStore.scope(store, "wardrik_events")
      written(store, 2)
      send(self(), {WardrikEvents, {"agt_1", nil}, %{"type" => "wardrik.message.delta"}})

      {:ok, stream} = WardrikEvents.stream(events, ["agt_1"], after: Cursor.encode(%{}))

      taken =
        stream
        |> Stream.each(fn {event, _cursor} ->
          if event["type"] == "wardrik.message.delta", do: written(store, 1)
        end)
        |> Enum.take(3)

      assert Enum.map(taken, &sequence/1) ==
               ["wardrik.message.delta", "000000000001", "000000000002"]
    end

    test "takes only the types asked for, and every gap", %{events: events} do
      stopped(events, "agt_1", ~U[2000-01-01 00:00:00Z])
      stopped(events, "agt_1")
      :ok = WardrikEvents.trim(events, ~U[2001-01-01 00:00:00Z])
      send(self(), {WardrikEvents, {"agt_1", nil}, %{"type" => "wardrik.message.delta"}})

      {:ok, stream} =
        WardrikEvents.stream(events, ["agt_1"],
          after: Cursor.encode(%{}),
          types: ["wardrik.message.delta"]
        )

      assert stream |> Enum.take(2) |> Enum.map(&sequence/1) ==
               ["wardrik.events.dropped", "wardrik.message.delta"]
    end

    test "refuses a cursor it didn't write", %{events: events} do
      assert {:error, :invalid_cursor} = WardrikEvents.stream(events, ["agt_1"], after: "nope")
    end

    test "closes after the events it follows have stopped", %{events: events} do
      stopped(events, "agt_1")
      {:ok, stream} = WardrikEvents.stream(events, ["agt_1"], after: Cursor.encode(%{}))

      taken =
        stream
        |> Stream.each(fn _taken -> stop_supervised!(WardrikEvents) end)
        |> Enum.take(1)

      assert [{%{"sequence" => "000000000001"}, _cursor}] = taken
    end
  end
end
