defmodule WardrikEvents.TailTest do
  use ExUnit.Case, async: false

  import WardrikEvents.Test.Events

  alias WardrikEvents.Cursor

  setup :start_events

  defp sequence({event, _cursor}), do: event["sequence"] || event["type"]

  defp following(stream, events, sequence) do
    Stream.each(stream, fn
      {%{"sequence" => ^sequence}, _cursor} -> stopped(events, "acme/triage")
      _other -> :ok
    end)
  end

  defp written(store, sequence) do
    number = sequence |> Integer.to_string() |> String.pad_leading(12, "0")
    event = %{"type" => "wardrik.agent.stopped", "sequence" => number}
    {:ok, _vsn} = WardrikStore.put(store, "events/acme%2Ftriage/" <> number, event)
    send(self(), {WardrikEvents, "acme/triage", event})
  end

  describe "subscribing" do
    test "sends each event to the processes whose patterns cover its log", %{events: events} do
      :ok = WardrikEvents.subscribe(events, ["acme/*", "*/triage"])

      for agent <- ["acme/triage", "acme/digest", "other/triage", "other/digest"],
          do: stopped(events, agent)

      assert_received {WardrikEvents, "acme/triage", _event}
      assert_received {WardrikEvents, "acme/digest", _event}
      assert_received {WardrikEvents, "other/triage", _event}
      refute_received {WardrikEvents, "acme/triage", _event}
      refute_received {WardrikEvents, "other/digest", _event}
    end

    test "stops sending once unsubscribed", %{events: events} do
      :ok = WardrikEvents.subscribe(events, ["acme/*", "*/triage"])
      :ok = WardrikEvents.unsubscribe(events, ["acme/*"])
      stopped(events, "acme/digest")
      stopped(events, "acme/triage")

      refute_received {WardrikEvents, "acme/digest", _event}
      assert_received {WardrikEvents, "acme/triage", _event}

      :ok = WardrikEvents.unsubscribe(events, ["*/triage"])
      stopped(events, "acme/triage")

      refute_received {WardrikEvents, "acme/triage", _event}
    end
  end

  describe "streaming" do
    test "reads on from a cursor, then follows what's written", %{events: events} do
      stopped(events, "acme/triage")
      stopped(events, "acme/triage")
      cursor = Cursor.encode(%{"acme/triage" => 1})

      {:ok, stream} = WardrikEvents.stream(events, ["acme/*"], after: cursor)

      taken = stream |> following(events, "000000000002") |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["000000000002", "000000000003"]
      assert {_event, %{"acme/triage" => 3}} = List.last(taken)
      refute_received {WardrikEvents, _log, _event}
    end

    test "starts at what's written next without a cursor", %{events: events} do
      stopped(events, "acme/triage")
      send(self(), {WardrikEvents, "acme/triage", %{"type" => "wardrik.message.delta"}})

      {:ok, stream} = WardrikEvents.stream(events, ["acme/*"])

      taken =
        stream
        |> Stream.each(fn {event, _cursor} ->
          if event["type"] == "wardrik.message.delta", do: stopped(events, "acme/digest")
        end)
        |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["wardrik.message.delta", "000000000001"]

      assert [{_delta, head}, {%{"source" => "wardrik://test/agents/acme/digest"}, _cursor}] =
               taken

      assert %{"acme/triage" => 1} = head
    end

    test "takes each kept event once", %{events: events} do
      stopped(events, "acme/triage")
      {:ok, %{events: [first]}} = WardrikEvents.page(events, ["acme/triage"])
      send(self(), {WardrikEvents, "acme/triage", first})

      {:ok, stream} = WardrikEvents.stream(events, ["acme/*"], after: Cursor.encode(%{}))

      taken = stream |> following(events, "000000000001") |> Enum.take(2)

      assert Enum.map(taken, &sequence/1) == ["000000000001", "000000000002"]
    end

    test "waits for an event written ahead of one still being written", %{
      events: events,
      store: store
    } do
      store = WardrikStore.scope(store, "wardrik_events")
      written(store, 2)
      send(self(), {WardrikEvents, "acme/triage", %{"type" => "wardrik.message.delta"}})

      {:ok, stream} = WardrikEvents.stream(events, ["acme/*"], after: Cursor.encode(%{}))

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
      stopped(events, "acme/triage", ~U[2000-01-01 00:00:00Z])
      stopped(events, "acme/triage")
      :ok = WardrikEvents.trim(events, ~U[2001-01-01 00:00:00Z])
      send(self(), {WardrikEvents, "acme/triage", %{"type" => "wardrik.message.delta"}})

      {:ok, stream} =
        WardrikEvents.stream(events, ["acme/*"],
          after: Cursor.encode(%{}),
          types: ["wardrik.message.delta"]
        )

      assert stream |> Enum.take(2) |> Enum.map(&sequence/1) ==
               ["wardrik.events.dropped", "wardrik.message.delta"]
    end

    test "refuses a cursor it didn't write", %{events: events} do
      assert {:error, :invalid_cursor} = WardrikEvents.stream(events, ["acme/*"], after: "nope")
    end

    test "closes after the events it follows have stopped", %{events: events} do
      stopped(events, "acme/triage")
      {:ok, stream} = WardrikEvents.stream(events, ["acme/*"], after: Cursor.encode(%{}))

      taken =
        stream
        |> Stream.each(fn _taken -> stop_supervised!(WardrikEvents) end)
        |> Enum.take(1)

      assert [{%{"sequence" => "000000000001"}, _cursor}] = taken
    end
  end
end
