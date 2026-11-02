defmodule WardrikEvents.AgentLogTest do
  use ExUnit.Case, async: false
  use Mimic

  import WardrikEvents.Test.LLM
  import WardrikEvents.Test.Protocol

  alias WardrikEvents.Runs
  alias WardrikEvents.Timeline

  setup :set_mimic_global

  setup do
    stub(ReqLLM.StreamResponse, :process_stream, fn response, _opts -> {:ok, response} end)
    store = WardrikStore.ETS.new(name: :agent_log_store)
    start_supervised!({WardrikStore, store})
    start_supervised!({WardrikEvents, name: :agent_log_events, store: store})
    start_supervised!({WardrikCore.Backends.MemoryStore, name: :agent_log_agents})

    start_supervised!(
      {WardrikCore,
       name: :agent_log_core,
       prefix: "log/",
       backend: {WardrikCore.Backends.MemoryStore, name: :agent_log_agents}}
    )

    :ok
  end

  defp streamed(acc \\ []) do
    receive do
      {WardrikEvents, _log, %{"type" => "wardrik.run.completed"} = event} ->
        Enum.reverse([event | acc])

      {WardrikEvents, _log, event} ->
        streamed([event | acc])
    after
      1_000 -> flunk("The run never completed: #{inspect(Enum.map(acc, & &1["type"]))}")
    end
  end

  defp doing(%{"data" => %{"activity" => %{"doing" => doing}}}),
    do: Enum.map(doing, & &1["type"])

  test "an agent's turn streams as it happens, and is kept in its log in order" do
    expect(ReqLLM, :stream_text, fn _model, _messages, _opts -> {:ok, answer("Hi.")} end)
    :ok = WardrikEvents.subscribe(:agent_log_events, ["agt_1"])

    {:ok, agent} =
      WardrikCore.start_agent(:agent_log_core,
        agent_id: "agt_1",
        model: "anthropic:claude-haiku-4-5",
        events: {WardrikEvents, :agent_log_events}
      )

    :ok = WardrikCore.ask(agent, "Say hi")
    {kept_streamed, live} = streamed() |> Enum.split_with(&Map.has_key?(&1, "sequence"))

    assert {:ok, %{events: kept}} = WardrikEvents.page(:agent_log_events, ["agt_1"])
    assert kept == kept_streamed

    assert Enum.map(kept, & &1["type"]) == [
             "wardrik.agent.started",
             "wardrik.message.created",
             "wardrik.run.started",
             "wardrik.message.started",
             "wardrik.message.completed",
             "wardrik.run.completed"
           ]

    assert Enum.map(kept, &String.to_integer(&1["sequence"])) == Enum.to_list(1..6)

    assert live |> Enum.map(&valid!/1) |> Enum.map(&doing/1) == [
             [],
             ["wardrik.run.started"],
             ["wardrik.message.started"]
           ]

    assert_receive {WardrikEvents, {"agt_1", nil},
                    %{"type" => "wardrik.activity.changed", "data" => %{"activity" => idle}}}

    assert %{"doing" => [], "pending" => 0} = idle
    assert WardrikEvents.activity(:agent_log_events, "agt_1") == idle
  end

  test "a conversation's turn is kept in the conversation's own log" do
    expect(ReqLLM, :stream_text, fn _model, _messages, _opts -> {:ok, answer("Hi.")} end)
    :ok = WardrikEvents.subscribe(:agent_log_events, [{"agt_1", "C1"}])

    {:ok, agent} =
      WardrikCore.start_agent(:agent_log_core,
        agent_id: "agt_1",
        conversation: "C1",
        model: "anthropic:claude-haiku-4-5",
        events: {WardrikEvents, :agent_log_events}
      )

    :ok = WardrikCore.ask(agent, "Say hi")
    streamed()

    assert {:ok, %{events: [started | _] = kept}} =
             WardrikEvents.page(:agent_log_events, [{"agt_1", "C1"}])

    assert Enum.all?(kept, &match?(%{"agent" => "agt_1", "conversation" => "C1"}, valid!(&1)))

    assert %{"type" => "wardrik.agent.started", "data" => %{"conversation" => %{"id" => "C1"}}} =
             started

    assert {:ok, %{events: []}} = WardrikEvents.page(:agent_log_events, [{"agt_1", nil}])
    assert {:ok, %{events: ^kept}} = WardrikEvents.page(:agent_log_events, ["agt_1"])
  end

  test "an agent's turn reads back as its timeline and its runs" do
    expect(ReqLLM, :stream_text, fn _model, _messages, _opts -> {:ok, answer("Hi.")} end)
    :ok = WardrikEvents.subscribe(:agent_log_events, ["agt_1"])

    {:ok, agent} =
      WardrikCore.start_agent(:agent_log_core,
        agent_id: "agt_1",
        model: "anthropic:claude-haiku-4-5",
        events: {WardrikEvents, :agent_log_events}
      )

    :ok = WardrikCore.ask(agent, "Say hi")
    streamed()

    assert {timeline, %{{"agt_1", nil} => 6}} =
             WardrikEvents.view(:agent_log_events, "agt_1", Timeline)

    assert [
             %{kind: :user, text: "Say hi"},
             %{
               kind: :run,
               status: :completed,
               model: "claude-haiku-4-5" <> _date,
               steps: [answer]
             }
           ] = Timeline.items(timeline)

    assert %{type: :text, text: "Hi.", live: false} = answer

    {runs, _cursor} = WardrikEvents.view(:agent_log_events, {"agt_1", nil}, Runs)

    assert [%{status: :completed, model: "claude-haiku-4-5" <> _date, source: nil} = run] =
             Runs.runs(runs)

    assert is_integer(Runs.duration_ms(run))
    assert %{runs: 1, failed: 0} = Runs.totals(runs)
  end
end
