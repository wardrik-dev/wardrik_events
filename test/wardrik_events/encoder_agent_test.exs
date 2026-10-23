defmodule WardrikEvents.EncoderAgentTest do
  use ExUnit.Case, async: false
  use Mimic

  import WardrikEvents.Test.LLM

  alias WardrikCore.Event

  setup_all do
    start_supervised!({WardrikCore.Backends.MemoryStore, name: :encoder_agents})

    start_supervised!(
      {WardrikCore,
       name: :encoder_core,
       prefix: "encoder/",
       backend: {WardrikCore.Backends.MemoryStore, name: :encoder_agents}}
    )

    :ok
  end

  setup :set_mimic_global

  setup do
    stub(ReqLLM.StreamResponse, :process_stream, fn response, _opts -> {:ok, response} end)
    :ok
  end

  defp received(acc \\ []) do
    receive do
      {:event, %Event{name: [:wardrik_core, :turn, :stop]} = event} -> Enum.reverse([event | acc])
      {:event, event} -> received([event | acc])
    after
      1_000 -> flunk("The turn never stopped: #{inspect(Enum.map(acc, & &1.name))}")
    end
  end

  test "every event of a real turn encodes as the protocol says" do
    ReqLLM
    |> expect(:stream_text, fn _model, _messages, _opts ->
      {:ok, calling([call("c1", "echo", %{text: "hi"})])}
    end)
    |> expect(:stream_text, fn _model, _messages, _opts -> {:ok, answer("Said hi.")} end)

    {:ok, agent} =
      WardrikCore.start_agent(:encoder_core,
        agent_id: "acme/encoded-#{System.unique_integer([:positive])}",
        model: "anthropic:claude-haiku-4-5",
        capabilities: [WardrikEvents.Test.Echo],
        events: {WardrikEvents.Test.Sink, self()}
      )

    :ok = WardrikCore.ask(agent, "Say hi", %{principal: %WardrikCore.Principal{id: "user:jon"}})

    events = received()
    envelope = JSV.build!(WardrikProtocol.schema("cloudevent"))

    types =
      for event <- events, cloud_event <- WardrikEvents.Encoder.encode(event, "local") do
        assert {:ok, _} = JSV.validate(cloud_event, envelope)
        "wardrik." <> type = cloud_event["type"]

        assert {:ok, _} =
                 JSV.validate(cloud_event["data"], JSV.build!(WardrikProtocol.schema(type)))

        type
      end

    assert types == [
             "agent.started",
             "message.created",
             "run.started",
             "message.started",
             "message.completed",
             "tool_call.started",
             "tool_call.completed",
             "message.started",
             "message.completed",
             "run.completed"
           ]
  end
end
