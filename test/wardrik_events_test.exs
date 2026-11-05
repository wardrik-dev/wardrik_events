defmodule WardrikEventsTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  import WardrikEvents.Test.Events
  import WardrikEvents.Test.Protocol

  alias WardrikEvents.Cursor
  alias WardrikEvents.Log

  setup :start_events

  defp failing_once do
    stub(WardrikStore, :put, fn store, key, value, opts ->
      call_original(WardrikStore, :put, [store, key, value, opts])
    end)

    expect(WardrikStore, :put, fn _store, _key, _value, _opts -> {:error, :down} end)
  end

  test "keeps each agent's events in order, numbered from 1 with no gaps", %{events: events} do
    for _n <- 1..3, do: stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) == ["000000000001", "000000000002", "000000000003"]
  end

  test "numbers each conversation's log apart from the agent's", %{events: events} do
    stopped(events, "agt_1")
    stopped(events, {"agt_1", "C1"})
    stopped(events, {"agt_1", "C1"})

    assert sequences(events, [{"agt_1", nil}]) == ["000000000001"]
    assert sequences(events, [{"agt_1", "C1"}]) == ["000000000001", "000000000002"]
  end

  test "carries on numbering after a restart", %{events: events, store: store} do
    stopped(events, "agt_1")
    stopped(events, "agt_1")

    stop_supervised!(WardrikEvents)
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) |> List.last() == "000000000003"
  end

  test "rebuilds activity from the log after a restart", %{events: events, store: store} do
    interrupted = %{"interrupts" => [%{"id" => "call_1"}]}
    :ok = WardrikEvents.append(events, "agt_1", {"message.created", "agents/agt_1", %{}})
    :ok = WardrikEvents.append(events, "agt_1", {"run.interrupted", "agents/agt_1", interrupted})
    parked = WardrikEvents.activity(events, "agt_1")

    stop_supervised!(WardrikEvents)
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})

    assert WardrikEvents.activity(events, "agt_1") == parked
    assert %{"pending" => 1, "doing" => [%{"interrupts" => [%{"id" => "call_1"}]}]} = parked

    stop_supervised!(WardrikEvents)
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    :ok = WardrikEvents.append(events, "agt_1", {"message.created", "agents/agt_1", %{}})

    assert %{"pending" => 2, "doing" => [%{"type" => "wardrik.run.interrupted"}]} =
             WardrikEvents.activity(events, "agt_1")
  end

  test "follows a log another node writes to, as when an agent moves", %{
    events: events,
    store: store
  } do
    other = elsewhere(events, store)
    interrupted = %{"interrupts" => [%{"id" => "call_1"}]}

    :ok = WardrikEvents.append(events, "agt_1", {"message.created", "agents/agt_1", %{}})
    :ok = WardrikEvents.append(events, "agt_1", {"run.interrupted", "agents/agt_1", interrupted})

    assert %{"doing" => [%{"interrupts" => _interrupts}]} =
             WardrikEvents.activity(events, "agt_1")

    assert WardrikEvents.activity(other, "agt_1") == WardrikEvents.activity(events, "agt_1")

    :ok = WardrikEvents.append(other, "agt_1", {"run.completed", "agents/agt_1", %{}})
    assert %{"doing" => []} = WardrikEvents.activity(events, "agt_1")

    :ok = WardrikEvents.append(events, "agt_1", {"message.created", "agents/agt_1", %{}})
    assert %{"pending" => 2} = WardrikEvents.activity(events, "agt_1")
    assert WardrikEvents.activity(other, "agt_1") == WardrikEvents.activity(events, "agt_1")
  end

  test "folds what retention kept when it dropped events this node never read", %{
    events: events,
    store: store
  } do
    other = elsewhere(events, store)
    :ok = WardrikEvents.append(events, "agt_1", {"message.created", "agents/agt_1", %{}})
    assert %{"pending" => 1} = WardrikEvents.activity(events, "agt_1")

    :ok = WardrikEvents.append(other, "agt_1", {"message.created", "agents/agt_1", %{}})
    WardrikEvents.trim(other, DateTime.add(DateTime.utc_now(), 1))
    :ok = WardrikEvents.append(other, "agt_1", {"message.created", "agents/agt_1", %{}})
    :ok = WardrikEvents.append(other, "agt_1", {"message.created", "agents/agt_1", %{}})

    assert %{"pending" => 2} = WardrikEvents.activity(events, "agt_1")
  end

  # A second events instance over the same store stands in for another node.
  defp elsewhere(events, store) do
    other = :"#{events} elsewhere"
    start_supervised!({WardrikEvents, name: other, store: store, deployment: "test"}, id: other)
    other
  end

  test "sends live events to the log's stream, and keeps them nowhere", %{events: events} do
    :ok = WardrikEvents.subscribe(events, ["agt_1"])
    span = WardrikCore.Span.root()
    emit(events, "agt_1", [:message, :delta], %{kind: :content, span: span, text: "Hi"})

    assert_receive {WardrikEvents, {"agt_1", nil}, %{"type" => "wardrik.message.delta"} = delta}
    refute Map.has_key?(delta, "sequence")
    assert sequences(events, ["agt_1"]) == []
  end

  test "sends kept events to the stream with their place in the log", %{events: events} do
    :ok = WardrikEvents.subscribe(events, ["agt_1"])
    stopped(events, "agt_1")

    assert_receive {WardrikEvents, {"agt_1", nil},
                    %{"type" => "wardrik.agent.stopped", "sequence" => "000000000001"}}
  end

  test "writes what the host appends into a log, as it writes core's", %{events: events} do
    :ok = WardrikEvents.subscribe(events, ["agt_1"])
    data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}

    assert :ok =
             WardrikEvents.append(
               events,
               {"agt_1", nil},
               {"trigger.fired", "triggers/daily", data}
             )

    assert_receive {WardrikEvents, {"agt_1", nil},
                    %{
                      "type" => "wardrik.trigger.fired",
                      "subject" => "triggers/daily",
                      "source" => "wardrik://test/agents/agt_1",
                      "agent" => "agt_1",
                      "sequence" => "000000000001",
                      "data" => ^data
                    } = event}

    refute Map.has_key?(valid!(event), "conversation")
  end

  test "names the agent and conversation of a conversation's event", %{events: events} do
    :ok = WardrikEvents.subscribe(events, ["a/b c%"])
    stopped(events, {"a/b c%", "C/1"})

    assert_receive {WardrikEvents, {"a/b c%", "C/1"}, event}

    assert %{
             "agent" => "a/b c%",
             "conversation" => "C/1",
             "source" => "wardrik://test/agents/a%2Fb%20c%25"
           } = valid!(event)
  end

  test "takes the next number when another writer took this one", %{events: events, store: store} do
    stopped(events, "agt_1")
    taken = WardrikStore.scope(store, "wardrik_events")

    {:ok, _vsn} =
      WardrikStore.put(
        taken,
        "events/agt_1/000000000002",
        %{"sequence" => "000000000002"},
        if_vsn: nil
      )

    stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) == ["000000000001", "000000000002", "000000000003"]
  end

  test "keeps nothing for a log it couldn't list", %{events: events} do
    failing_once()

    assert capture_log(fn -> stopped(events, "agt_1") end) =~
             ~s(Couldn't keep wardrik.agent.stopped for {"agt_1", nil})

    stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) == ["000000000001"]
  end

  test "leaves no gap when an event's write fails", %{events: events} do
    stopped(events, "agt_1")
    failing_once()

    assert capture_log(fn -> stopped(events, "agt_1") end) =~
             "Couldn't keep wardrik.agent.stopped"

    stopped(events, "agt_1")

    assert sequences(events, ["agt_1"]) == ["000000000001", "000000000002"]
  end

  describe "covers?/2" do
    test "an agent covers its own log and its conversations'" do
      assert WardrikEvents.covers?("agt_1", {"agt_1", nil})
      assert WardrikEvents.covers?("agt_1", {"agt_1", "C1"})
      refute WardrikEvents.covers?("agt_1", {"agt_10", nil})
      refute WardrikEvents.covers?("agt_10", {"agt_1", "0"})
    end

    test "a log covers only itself" do
      assert WardrikEvents.covers?({"agt_1", "C1"}, {"agt_1", "C1"})
      assert WardrikEvents.covers?({"agt_1", nil}, {"agt_1", nil})
      refute WardrikEvents.covers?({"agt_1", nil}, {"agt_1", "C1"})
      refute WardrikEvents.covers?({"agt_1", "C1"}, {"agt_1", "C2"})
    end

    test ":all covers every log" do
      assert WardrikEvents.covers?(:all, {"agt_1", nil})
      assert WardrikEvents.covers?(:all, {"agt_2", "C1"})
    end
  end

  describe "reading" do
    setup %{events: events} do
      for log <- ["agt_1", {"agt_1", "C1"}, "agt_10", "agt_2"],
          _n <- 1..2,
          do: stopped(events, log)

      :ok
    end

    test "starts a reader at the end of each log an agent covers", %{events: events} do
      assert {:ok, head} = Cursor.decode(WardrikEvents.head(events, ["agt_1"]))
      assert head == %{{"agt_1", nil} => 2, {"agt_1", "C1"} => 2}

      assert {:ok, %{events: []}} =
               WardrikEvents.page(events, ["agt_1"], after: WardrikEvents.head(events, ["agt_1"]))
    end

    test "pages through every log an agent covers, and no other agent's", %{events: events} do
      assert {:ok, %{events: first, next: next}} = WardrikEvents.page(events, ["agt_1"], limit: 3)
      assert [_, _, _] = first

      assert {:ok, %{events: rest}} = WardrikEvents.page(events, ["agt_1"], after: next)
      assert [_] = rest

      read = first ++ rest
      assert Enum.all?(read, &(&1["source"] == "wardrik://test/agents/agt_1"))

      assert read |> Enum.map(& &1["conversation"]) |> Enum.frequencies() == %{
               nil => 2,
               "C1" => 2
             }
    end

    test "reads only the exact logs named", %{events: events} do
      assert [_, _] = sequences(events, [{"agt_1", "C1"}])
      assert [_, _, _, _] = sequences(events, [{"agt_1", "C1"}, {"agt_2", nil}])
      assert [] = sequences(events, [{"agt_1", "C2"}, {"agt", nil}])
    end

    test "reads several agents together", %{events: events} do
      assert [_, _, _, _, _, _, _, _] = sequences(events, ["agt_1", "agt_10", "agt_2"])
    end

    test "says which events aren't started" do
      assert_raise ArgumentError, ~r/^MyApp.Missing isn't started/, fn ->
        WardrikEvents.head(MyApp.Missing, ["agt_1"])
      end
    end

    test "reads each log once, however many resources cover it", %{events: events} do
      assert [_, _, _, _] = sequences(events, ["agt_1", {"agt_1", "C1"}, "agt_1"])
    end

    test "refuses a cursor it didn't write", %{events: events} do
      assert {:error, :invalid_cursor} =
               WardrikEvents.page(events, ["agt_1"], after: "not a cursor")

      for json <- [
            ~s({"agt_1": 1}),
            ~s([["agt_1", null, -1]]),
            ~s([["agt_1", 1, 1]]),
            ~s([[null, null, 1]]),
            ~s([["agt_1", 1]]),
            "[1]"
          ] do
        assert {:error, :invalid_cursor} = Cursor.decode(Base.url_encode64(json, padding: false))
      end
    end
  end

  describe "IDs holding any text" do
    @odd [{"a/b", nil}, {"a/b", "c/d"}, {"a%2Fb", "%"}, {"a b+c", "x y"}, {"a", "b/c"}]

    test "round-trip through the store's keys, the logs and cursors", %{
      events: events,
      store: store
    } do
      for log <- @odd, do: stopped(events, log)

      assert store |> WardrikStore.scope("wardrik_events") |> Log.logs() |> Enum.sort() ==
               Enum.sort(@odd)

      for log <- @odd do
        assert sequences(events, [log]) == ["000000000001"]
        assert {:ok, %{^log => 1}} = Cursor.decode(WardrikEvents.head(events, [log]))
      end

      assert {:ok, head} = Cursor.decode(WardrikEvents.head(events, @odd))
      assert head == Map.new(@odd, &{&1, 1})
      assert {:ok, ^head} = head |> Cursor.encode() |> Cursor.decode()
    end

    test "keep each agent's logs apart from an agent whose ID starts the same", %{
      events: events,
      store: store
    } do
      for log <- @odd, do: stopped(events, log)
      logs = WardrikStore.scope(store, "wardrik_events")

      assert Enum.sort(Log.logs(logs, "a/b")) == [{"a/b", nil}, {"a/b", "c/d"}]
      assert Log.logs(logs, "a") == [{"a", "b/c"}]
      assert [_, _] = sequences(events, ["a/b"])
      assert [_] = sequences(events, ["a"])
    end
  end
end
