defmodule WardrikEvents.EncoderTest do
  use ExUnit.Case, async: true

  import WardrikEvents.Test.Protocol

  alias WardrikCore.Event
  alias WardrikCore.Principal
  alias WardrikCore.Span
  alias WardrikCore.ToolCall
  alias WardrikEvents.Encoder

  @agent "acme/triage"

  setup do
    turn = Span.root()
    %{turn: turn, step: Span.child(turn)}
  end

  defp event(name, metadata, measurements \\ %{}) do
    %Event{
      name: name,
      time: DateTime.utc_now(),
      metadata: Map.put(metadata, :agent_id, @agent),
      measurements: measurements
    }
  end

  defp core(name, metadata, measurements \\ %{}),
    do: event([:wardrik_core | name], metadata, measurements)

  defp encode(event) do
    for cloud_event <- Encoder.encode(event, "local"), do: valid!(cloud_event)
  end

  defp one(event) do
    assert [cloud_event] = encode(event)
    cloud_event
  end

  defp response(message, extra \\ []),
    do:
      struct!(
        ReqLLM.Response,
        [id: "resp_1", model: "claude-haiku-4-5", context: nil, message: message] ++ extra
      )

  defp duration(ms), do: %{duration: System.convert_time_unit(ms, :millisecond, :native)}

  describe "the envelope" do
    test "names the agent, a UUIDv7, the span and the one above it", %{step: step, turn: turn} do
      event = one(core([:step, :start], %{step: {:step, "summarize"}, span: step}))

      assert event["source"] == "wardrik://local/agents/acme/triage"

      assert event["id"] =~
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

      assert event["traceparent"] == "00-#{turn.trace}-#{step.id}-01"
      assert event["parentspanid"] == turn.id
      assert event["dataschema"] == "urn:wardrik:schema:v1:step.started"
    end

    test "starts a trace of its own for an event outside a turn" do
      event = one(core([:agent, :stop], %{reason: :normal}))

      assert event["traceparent"] =~ ~r/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/
      refute Map.has_key?(event, "parentspanid")
    end

    test "puts the event's place in its log" do
      event =
        core([:agent, :stop], %{reason: :normal})
        |> one()
        |> WardrikEvents.CloudEvent.sequenced(42)

      assert event["sequence"] == "000000000042"
    end
  end

  describe "the agent and its messages" do
    test "says the agent started, and how it stands" do
      assert %{"type" => "wardrik.agent.started", "data" => %{"status" => "parked"}} =
               one(core([:agent, :start], %{state: :waiting}))

      assert %{"data" => %{"status" => "running"}} = one(core([:agent, :start], %{state: :due}))
    end

    test "says who sent a message" do
      jon = %Principal{id: "user:jon", attributes: %{name: "Jon"}}
      message = ReqLLM.Context.user("What changed?", %{id: "msg_1", principal: jon})

      assert %{
               "type" => "wardrik.message.created",
               "subject" => "messages/msg_1",
               "data" => %{
                 "message" => %{
                   "id" => "msg_1",
                   "role" => "user",
                   "parts" => [%{"type" => "text", "content" => "What changed?"}]
                 },
                 "sender" => %{"id" => "user:jon", "name" => "Jon"}
               }
             } = one(core([:message, :queued], %{message: message}))
    end
  end

  describe "runs" do
    test "start, resume, and carry on after a restart with nothing to say", %{turn: turn} do
      assert %{
               "type" => "wardrik.run.started",
               "subject" => "runs/" <> trace,
               "data" => %{"run" => %{"message_ids" => ["msg_1"]}}
             } = one(core([:turn, :start], %{kind: :run, span: turn, message_ids: ["msg_1"]}))

      assert trace == turn.trace

      assert %{"type" => "wardrik.run.resumed"} =
               one(core([:turn, :start], %{kind: :resume, span: turn}))

      assert [] = encode(core([:turn, :start], %{kind: :recover, span: turn}))
    end

    test "complete with what they answered", %{turn: turn} do
      messages = [ReqLLM.Context.assistant("Three commits landed.")]

      assert %{
               "type" => "wardrik.run.completed",
               "data" => %{"messages" => [%{"role" => "assistant"}]}
             } =
               one(
                 core([:turn, :stop], %{kind: :run, span: turn, outcome: :ok, messages: messages})
               )
    end

    test "list what they wait on", %{turn: turn} do
      parked = [
        {%ToolCall{id: "c1", name: "books__pay"},
         {:park, %{interrupt: "tool_approval", message: "Pay $1,200?", labels: ["outside"]}}},
        {%ToolCall{id: "c2", name: "github__list"},
         {:park, %WardrikCore.Authorization{url: "https://x.test/1", credential: "github"}}},
        {%ToolCall{id: "c3", name: "mail__send"}, {:park, :in_doubt}},
        {%ToolCall{id: "c4", name: "echo"}, {:ok, "done"}}
      ]

      %{"data" => %{"interrupts" => interrupts}} =
        one(
          core([:turn, :stop], %{
            kind: :run,
            span: turn,
            outcome: :park,
            reason: {:tool_calls, parked}
          })
        )

      assert [
               %{
                 "id" => "c1",
                 "reason" => "tool_approval",
                 "message" => "Pay $1,200?",
                 "name" => "books__pay",
                 "labels" => ["outside"]
               },
               %{
                 "id" => "c2",
                 "reason" => "authorization",
                 "message" => "Sign in to github at https://x.test/1."
               },
               %{"id" => "c3", "reason" => "in_doubt", "tool_call_id" => "c3"}
             ] = interrupts

      model = %{interrupt: "model_approval", message: "Over budget."}

      assert %{
               "data" => %{
                 "interrupts" => [%{"id" => "model", "reason" => "model_approval"} = asked]
               }
             } =
               one(core([:turn, :stop], %{kind: :run, span: turn, outcome: :park, reason: model}))

      refute Map.has_key?(asked, "tool_call_id")
      refute Map.has_key?(asked, "labels")
    end

    test "are cancelled, reach their deadline, or fail", %{turn: turn} do
      stopped = %{kind: :run, span: turn, kept: 2, dropped: 0}

      assert %{
               "type" => "wardrik.run.cancelled",
               "data" => %{"kept" => 2, "dropped" => 0, "duration_ms" => 1500}
             } =
               one(core([:turn, :stop], Map.put(stopped, :outcome, :interrupted), duration(1500)))

      assert %{"type" => "wardrik.run.deadline_exceeded", "data" => %{"message_count" => 2}} =
               one(core([:turn, :stop], Map.put(stopped, :outcome, :timeout), duration(10)))

      assert %{
               "type" => "wardrik.run.failed",
               "data" => %{"error" => %{"type" => "rate_limited"}}
             } =
               one(
                 core([:turn, :stop], %{
                   kind: :run,
                   span: turn,
                   outcome: :error,
                   reason: {:rate_limited, 30}
                 })
               )

      assert %{"data" => %{"error" => %{"type" => "crash"}}} =
               one(core([:turn, :stop], %{kind: :run, span: turn, outcome: :crash}))
    end
  end

  describe "model calls" do
    test "start, stream, and complete as one message", %{step: step} do
      id = "msg_" <> step.id

      assert %{
               "type" => "wardrik.message.started",
               "data" => %{
                 "message" => %{"id" => ^id},
                 "request" => %{"model" => "claude-haiku-4-5"},
                 "provider" => %{"name" => "anthropic"}
               }
             } =
               one(
                 core([:step, :start], %{
                   step: :generate,
                   span: step,
                   model: "anthropic:claude-haiku-4-5"
                 })
               )

      assert %{
               "type" => "wardrik.message.delta",
               "data" => %{"message" => %{"id" => ^id}, "delta" => "Thr"}
             } =
               one(core([:message, :delta], %{kind: :content, span: step, text: "Thr"}))

      assert %{"type" => "wardrik.reasoning.delta"} =
               one(core([:message, :delta], %{kind: :thinking, span: step, text: "hm"}))

      call = ReqLLM.ToolCall.new("c1", "github__list", ~s({"repo":"wardrik"}))
      answer = ReqLLM.Context.assistant("", tool_calls: [call])

      assert %{
               "type" => "wardrik.message.completed",
               "data" => %{
                 "status" => "ok",
                 "message" => %{
                   "id" => ^id,
                   "parts" => [
                     %{"type" => "tool_call", "id" => "c1", "arguments" => %{"repo" => "wardrik"}}
                   ]
                 },
                 "response" => %{
                   "model" => "claude-haiku-4-5",
                   "finish_reasons" => ["tool_calls"]
                 },
                 "usage" => %{"input_tokens" => 120, "output_tokens" => 30},
                 "cost" => %{"micro_usd" => 1500, "source" => "catalog"}
               }
             } =
               one(
                 core(
                   [:step, :stop],
                   %{
                     step: :generate,
                     span: step,
                     status: :ok,
                     response: response(answer, finish_reason: :tool_calls)
                   },
                   Map.merge(duration(800), %{input_tokens: 120, output_tokens: 30, usd: 0.0015})
                 )
               )
    end

    test "cost null when nobody prices the model, and nothing when it never answered", %{
      step: step
    } do
      answered = %{
        step: :generate,
        span: step,
        status: :ok,
        response: response(ReqLLM.Context.assistant("hi"))
      }

      assert %{"data" => %{"cost" => nil}} =
               one(core([:step, :stop], answered, Map.put(duration(5), :unpriced_calls, 1)))

      failed =
        core(
          [:step, :stop],
          %{step: :generate, span: step, status: :error, reason: :timeout},
          duration(5)
        )

      assert %{"data" => %{"status" => "error", "error" => %{"type" => "timeout"}} = data} =
               one(failed)

      refute Map.has_key?(data, "cost")
    end
  end

  describe "tool calls and steps" do
    test "end in an error when a call is stopped, or dies" do
      call = %ToolCall{id: "c7", name: "block"}

      assert %{
               "type" => "wardrik.tool_call.completed",
               "subject" => "tool_calls/c7",
               "data" => %{
                 "status" => "error",
                 "error" => %{"message" => "The call was stopped before it finished."}
               }
             } = one(core([:tool_call, :exit], %{call: call, reason: :killed}))

      assert %{"data" => %{"status" => "error", "error" => %{"message" => message}}} =
               one(core([:tool_call, :exit], %{call: call, reason: :timeout}))

      assert message =~ "timeout"
    end

    test "start and complete, with what the tool cost", %{step: step} do
      call = %ToolCall{id: "c1", name: "github__list", arguments: %{"repo" => "wardrik"}}

      assert %{
               "type" => "wardrik.tool_call.started",
               "subject" => "tool_calls/c1",
               "data" => %{"decided" => %{"by" => "default"}}
             } =
               one(core([:step, :start], %{step: {:tool_call, call}, span: step}))

      assert %{
               "data" => %{
                 "status" => "ok",
                 "tool" => %{"call" => %{"result" => "3 commits"}},
                 "cost" => %{"micro_usd" => 10_000, "source" => "tool"}
               }
             } =
               one(
                 core(
                   [:step, :stop],
                   %{
                     step: {:tool_call, call},
                     span: step,
                     status: :ok,
                     response: {:ok, "3 commits"}
                   },
                   Map.put(duration(3), :usd, 0.01)
                 )
               )

      labelled = %ReqLLM.ToolResult{output: "a note", metadata: %{labels: ["outside"]}}

      assert %{"data" => %{"tool" => %{"call" => %{"result" => "a note"}}}} =
               one(
                 core([:step, :stop], %{
                   step: {:tool_call, call},
                   span: step,
                   status: :ok,
                   response: {:ok, labelled}
                 })
               )

      assert %{"data" => %{"status" => "error", "error" => %{"message" => "Not found"}}} =
               one(
                 core(
                   [:step, :stop],
                   %{
                     step: {:tool_call, call},
                     span: step,
                     status: :ok,
                     response: {:error, "Not found"}
                   },
                   duration(3)
                 )
               )

      assert [] =
               encode(
                 core([:step, :stop], %{
                   step: {:tool_call, call},
                   span: step,
                   status: :park,
                   reason: :in_doubt
                 })
               )
    end

    test "cut arguments past 64 KiB short, and say so", %{step: step} do
      call = %ToolCall{
        id: "c1",
        name: "write",
        arguments: %{"text" => String.duplicate("a", 70_000)}
      }

      assert %{
               "data" => %{
                 "tool" => %{"call" => %{"truncated" => true, "arguments" => arguments}}
               }
             } =
               one(core([:step, :start], %{step: {:tool_call, call}, span: step}))

      assert byte_size(arguments) == 64 * 1024
    end

    test "a named step completes, or fails when it raises", %{step: step} do
      assert %{"type" => "wardrik.step.completed", "data" => %{"status" => "ok"}} =
               one(
                 core(
                   [:step, :stop],
                   %{step: {:step, "summarize"}, span: step, status: :ok},
                   duration(2)
                 )
               )

      assert %{
               "data" => %{
                 "status" => "error",
                 "error" => %{"type" => "RuntimeError", "message" => "boom"}
               }
             } =
               one(
                 core(
                   [:step, :exception],
                   %{
                     step: {:step, "summarize"},
                     span: step,
                     kind: :error,
                     reason: %RuntimeError{message: "boom"}
                   },
                   duration(2)
                 )
               )

      assert [] =
               encode(
                 core(
                   [:step, :exception],
                   %{step: :turn, span: step, kind: :error, reason: :boom},
                   duration(2)
                 )
               )
    end
  end

  describe "events packages emit" do
    test "pass through as the protocol's, with their metadata as data", %{step: step} do
      denied = %{
        span: step,
        tool: %{name: "shell__exec", call: %{id: "c9"}},
        reason: "No shell.",
        decided: %{by: "hook"}
      }

      assert %{
               "type" => "wardrik.tool_call.denied",
               "subject" => "tool_calls/c9",
               "data" => %{"reason" => "No shell."} = data
             } =
               one(event([:wardrik, :tool_call, :denied], denied))

      refute Map.has_key?(data, "span")

      resolved = %{
        span: step,
        interrupt: %{id: "c1"},
        decision: "approved",
        resolved_by: %Principal{id: "user:maria"}
      }

      assert %{
               "subject" => "interrupts/c1",
               "data" => %{"resolved_by" => %{"id" => "user:maria"}}
             } =
               one(event([:wardrik, :interrupt, :resolved], resolved))
    end

    test "encode none that aren't the protocol's, or aren't an agent's" do
      assert [] = encode(event([:wardrik_policy, :decision], %{}))
      assert [] = encode(core([:turn, :checkpoint], %{}))

      assert [] =
               Encoder.encode(
                 %Event{name: [:wardrik, :run, :started], time: DateTime.utc_now()},
                 "local"
               )
    end
  end
end
