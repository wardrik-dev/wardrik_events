defmodule WardrikEventsTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias WardrikCore.Event
  alias WardrikEvents.Cursor

  setup context do
    store = WardrikStore.ETS.new(name: :"store #{context.test}")
    events = :"events #{context.test}"
    start_supervised!({WardrikStore, store})
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    %{events: events, store: store}
  end

  defp emit(events, agent_id, name, metadata) do
    event = %Event{
      name: [:wardrik_core | name],
      time: DateTime.utc_now(),
      metadata: Map.put(metadata, :agent_id, agent_id)
    }

    :ok = WardrikEvents.handle_event(event, events)
  end

  defp stopped(events, agent_id), do: emit(events, agent_id, [:agent, :stop], %{reason: :normal})

  defp sequences(events, resources, opts \\ []) do
    {:ok, %{events: read}} = WardrikEvents.page(events, resources, opts)
    Enum.map(read, & &1["sequence"])
  end

  test "keeps each agent's events in order, numbered from 1 with no gaps", %{events: events} do
    for _n <- 1..3, do: stopped(events, "acme/triage")

    assert sequences(events, ["acme/triage"]) == ["000000000001", "000000000002", "000000000003"]
  end

  test "carries on numbering after a restart", %{events: events, store: store} do
    stopped(events, "acme/triage")
    stopped(events, "acme/triage")

    stop_supervised!(WardrikEvents)
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    stopped(events, "acme/triage")

    assert sequences(events, ["acme/triage"]) |> List.last() == "000000000003"
  end

  test "sends live events to the log's stream, and keeps them nowhere", %{events: events} do
    :ok = WardrikEvents.join(events, "acme/triage")
    span = WardrikCore.Span.root()
    emit(events, "acme/triage", [:message, :delta], %{kind: :content, span: span, text: "Hi"})

    assert_receive {WardrikEvents, "acme/triage", %{"type" => "wardrik.message.delta"} = delta}
    refute Map.has_key?(delta, "sequence")
    assert sequences(events, ["acme/triage"]) == []
  end

  test "sends kept events to the stream with their place in the log", %{events: events} do
    :ok = WardrikEvents.join(events, "acme/triage")
    stopped(events, "acme/triage")

    assert_receive {WardrikEvents, "acme/triage",
                    %{"type" => "wardrik.agent.stopped", "sequence" => "000000000001"}}
  end

  test "takes the next number when another writer took this one", %{events: events, store: store} do
    stopped(events, "acme/triage")
    taken = WardrikStore.scope(store, "wardrik_events")

    {:ok, _vsn} =
      WardrikStore.put(taken, "acme%2Ftriage/000000000002", %{"sequence" => "000000000002"},
        if_vsn: nil
      )

    stopped(events, "acme/triage")

    assert sequences(events, ["acme/triage"]) == ["000000000001", "000000000002", "000000000003"]
  end

  test "leaves no gap when a write fails", %{events: events} do
    stub(WardrikStore, :put, fn store, key, value, opts ->
      call_original(WardrikStore, :put, [store, key, value, opts])
    end)

    expect(WardrikStore, :put, fn _store, _key, _value, _opts -> {:error, :down} end)

    assert capture_log(fn -> stopped(events, "acme/triage") end) =~
             "Couldn't keep wardrik.agent.stopped"

    stopped(events, "acme/triage")

    assert sequences(events, ["acme/triage"]) == ["000000000001"]
  end

  describe "reading" do
    setup %{events: events} do
      for agent <- ["acme/triage", "acme/digest", "other/triage"],
          _n <- 1..2,
          do: stopped(events, agent)

      :ok
    end

    test "starts a reader at the end of each log a pattern covers", %{events: events} do
      assert {:ok, %{"acme/triage" => 2, "acme/digest" => 2} = head} =
               Cursor.decode(WardrikEvents.head(events, ["acme/*"]))

      refute Map.has_key?(head, "other/triage")

      assert {:ok, %{events: []}} =
               WardrikEvents.page(events, ["acme/*"],
                 after: WardrikEvents.head(events, ["acme/*"])
               )
    end

    test "pages through every log a pattern covers", %{events: events} do
      assert {:ok, %{events: first, next: next}} =
               WardrikEvents.page(events, ["acme/*"], limit: 3)

      assert [_, _, _] = first

      assert {:ok, %{events: rest}} = WardrikEvents.page(events, ["acme/*"], after: next)
      assert [_] = rest

      assert Enum.all?(
               first ++ rest,
               &String.starts_with?(&1["source"], "wardrik://test/agents/acme/")
             )

      assert [_, _, _, _] = sequences(events, ["*/triage", "acme/digest?"])
    end

    test "refuses a cursor it didn't write", %{events: events} do
      assert {:error, :invalid_cursor} =
               WardrikEvents.page(events, ["acme/*"], after: "not a cursor")

      assert {:error, :invalid_cursor} =
               Cursor.decode(Base.url_encode64(~s({"acme/triage": -1}), padding: false))

      assert {:error, :invalid_cursor} = Cursor.decode(Base.url_encode64("[1]", padding: false))
    end
  end
end
