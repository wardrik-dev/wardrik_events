defmodule WardrikEvents.ViewsTest do
  use ExUnit.Case, async: true

  alias WardrikEvents.Activity
  alias WardrikEvents.Heads
  alias WardrikEvents.Runs
  alias WardrikEvents.Timeline
  alias WardrikEvents.View

  @run "0af7651916cd43dd8448eb211c80319c"
  @at "2026-10-22T09:00:00Z"

  defp event(type, data, time \\ @at) do
    %{
      "type" => "wardrik." <> type,
      "data" => data,
      "time" => time,
      "traceparent" => "00-#{@run}-b7ad6b7169203331-01"
    }
  end

  defp tool(id, extra \\ %{}),
    do: %{"tool" => %{"name" => "search", "call" => Map.put(extra, "id", id)}}

  defp run, do: %{"run" => %{"id" => @run, "message_ids" => ["msg_1"]}}

  defp doing(activity),
    do:
      Enum.map(
        activity["doing"],
        &(&1["type"] <> " " <> (get_in(&1, ["tool", "call", "id"]) || ""))
      )

  describe "activity" do
    test "is every tool call running, oldest first, then the last to settle" do
      activity =
        View.fold(
          [
            event("run.started", run()),
            event("tool_call.started", tool("a")),
            event("tool_call.started", tool("b"))
          ],
          Activity
        )

      assert doing(activity) == ["wardrik.tool_call.started a", "wardrik.tool_call.started b"]

      activity = Activity.handle_event(event("tool_call.completed", tool("a")), activity)
      assert doing(activity) == ["wardrik.tool_call.started b"]

      activity = Activity.handle_event(event("tool_call.denied", tool("b")), activity)
      assert doing(activity) == ["wardrik.tool_call.denied b"]

      activity = Activity.handle_event(event("tool_call.completed", tool("c")), activity)
      assert doing(activity) == ["wardrik.tool_call.completed c"]
    end

    test "counts the messages waiting, and goes idle when a run ends" do
      activity =
        View.fold(
          [
            event("message.created", %{"message" => %{"id" => "msg_1"}}),
            event("message.created", %{"message" => %{"id" => "msg_2"}}),
            event("run.started", run(), "2026-10-22T09:00:01Z"),
            event("run.interrupted", %{"interrupts" => [%{"id" => "model"}]})
          ],
          Activity,
          since: ~U[2026-10-22 08:00:00Z]
        )

      assert %{"pending" => 1, "doing" => [%{"interrupts" => [%{"id" => "model"}]}]} = activity

      activity =
        Activity.handle_event(
          event("run.cancelled", %{"dropped" => 3}, "2026-10-22T09:00:02Z"),
          activity
        )

      assert activity == %{"doing" => [], "pending" => 0, "since" => "2026-10-22T09:00:02Z"}
      assert Activity.handle_event(%{"type" => "wardrik.message.delta"}, activity) == activity
    end

    test "keeps its time when nothing it does changes" do
      activity = View.fold([event("step.started", %{"step" => %{"name" => "plan"}})], Activity)

      assert Activity.handle_event(event("message.delta", %{}, "later"), activity) == activity
    end
  end

  describe "timeline" do
    test "reads a run's thinking, answer and tool calls in order" do
      timeline =
        View.fold(
          [
            event("message.created", %{
              "message" => %{
                "id" => "msg_1",
                "role" => "user",
                "parts" => [%{"type" => "text", "content" => "Hi"}]
              },
              "sender" => %{"id" => "u1", "name" => "Ada"}
            }),
            event("run.started", run()),
            event("message.started", %{"request" => %{"model" => "haiku"}}),
            event("reasoning.delta", %{"message" => %{"id" => "m"}, "delta" => "Hm"}),
            event("message.delta", %{"message" => %{"id" => "m"}, "delta" => "Lo"}),
            event("tool_call.started", tool("a", %{"arguments" => %{"q" => "x"}})),
            event(
              "tool_call.completed",
              Map.merge(tool("a", %{"result" => "y"}), %{"status" => "ok"})
            ),
            event(
              "tool_call.completed",
              Map.merge(tool("b"), %{"status" => "error", "error" => %{"message" => "no"}})
            ),
            event("tool_call.denied", Map.put(tool("c"), "reason", "not allowed")),
            event("message.completed", %{
              "message" => %{"id" => "m", "parts" => [%{"type" => "text", "content" => "Hello"}]},
              "cost" => %{"micro_usd" => 7}
            }),
            event("run.completed", run())
          ],
          Timeline
        )

      assert [%{kind: :user, text: "Hi", sender: "Ada"}, run] = Timeline.items(timeline)
      assert %{status: :completed, model: "haiku", cost_micro_usd: 7, finished_at: @at} = run

      assert [
               %{type: :thinking, text: "Hm", live: false},
               %{type: :text, text: "Hello", live: false},
               %{type: :tool, status: :ok, result: "y", arguments: %{"q" => "x"}},
               %{type: :tool, status: :error, error: "no"},
               %{type: :tool, status: :denied, error: "not allowed"}
             ] = run.steps

      assert Timeline.touched(timeline) == ["run-" <> @run]
    end

    test "waits on a person, and runs on once they answer" do
      timeline =
        View.fold(
          [
            event("run.started", run()),
            event("run.interrupted", %{"interrupts" => [%{"id" => "a"}, %{"id" => "b"}]}),
            event("interrupt.resolved", %{"interrupt" => %{"id" => "a"}}),
            event("interrupt.resolved", %{"interrupt" => %{"id" => "z"}})
          ],
          Timeline
        )

      assert [%{status: :waiting, interrupts: [%{"id" => "b"}]}] = Timeline.items(timeline)

      timeline = Timeline.handle_event(event("run.resumed", %{}), timeline)
      assert [%{status: :running, interrupts: []}] = Timeline.items(timeline)
    end

    test "says why a run failed, briefly" do
      long = String.duplicate("x", 400)

      timeline =
        View.fold(
          [
            event("run.started", run()),
            event("message.delta", %{"message" => %{"id" => "m"}, "delta" => "Lo"}),
            event("message.completed", %{
              "message" => %{"id" => "m", "parts" => []},
              "error" => %{"type" => "timeout"}
            }),
            event("run.failed", %{"error" => %{"message" => long}}),
            Map.delete(event("tool_call.started", tool("x")), "traceparent"),
            %{"type" => "not an event"}
          ],
          Timeline
        )

      assert [%{status: :failed, error: error, steps: [%{text: "Lo", live: false}]}] =
               Timeline.items(timeline)

      assert String.length(error) == 301
      assert Timeline.touched(timeline) == []
    end
  end

  describe "runs" do
    test "sums each run, and says who started it" do
      runs =
        View.fold(
          [
            event("message.created", %{
              "message" => %{"id" => "msg_1"},
              "sender" => %{"id" => "u1", "name" => "Ada"}
            }),
            event("run.started", run(), "2026-10-22T09:00:00Z"),
            event("message.started", %{"request" => %{"model" => "haiku"}}),
            event("message.completed", %{
              "usage" => %{"input_tokens" => 10, "output_tokens" => 5},
              "cost" => %{"micro_usd" => 3}
            }),
            event("tool_call.started", tool("a")),
            event("tool_call.completed", Map.put(tool("a"), "status", "error")),
            event("tool_call.denied", tool("b")),
            event("run.interrupted", %{}),
            event("run.resumed", %{}),
            event("run.failed", %{"error" => %{"message" => "boom"}}, "2026-10-22T09:00:02Z"),
            %{"not" => "an event"}
          ],
          Runs
        )

      assert [run] = Runs.runs(runs)

      assert %{
               source: "Ada",
               model: "haiku",
               status: :failed,
               error: "boom",
               tool_calls: 1,
               tool_errors: 2,
               input_tokens: 10,
               output_tokens: 5,
               cost_micro_usd: 3
             } = run

      assert Runs.duration_ms(run) == 2_000

      assert Runs.totals(runs) == %{
               runs: 1,
               failed: 1,
               tool_calls: 1,
               input_tokens: 10,
               output_tokens: 5,
               cost_micro_usd: 3,
               last_at: "2026-10-22T09:00:00Z"
             }
    end

    test "has nothing to say before a run, or of a run going on" do
      runs = View.fold([event("message.delta", %{}), event("run.started", run())], Runs)

      assert [%{status: :running} = run] = Runs.runs(runs)
      assert Runs.duration_ms(run) == nil

      assert Runs.totals(Runs.init([])) == %{
               runs: 0,
               failed: 0,
               tool_calls: 0,
               input_tokens: 0,
               output_tokens: 0,
               cost_micro_usd: 0,
               last_at: nil
             }
    end
  end

  describe "an update in the heads table" do
    setup context do
      table = :"heads #{context.test}"
      start_supervised!({Heads, %{heads: table}})
      %{table: table}
    end

    test "applies again when another update came in between", %{table: table} do
      interleaved = fn count ->
        if count == 0, do: Heads.update(table, :key, 0, &(&1 + 10))
        count + 1
      end

      assert Heads.update(table, :key, 0, interleaved) == {10, 11}
      assert Heads.update(table, :key, 0, &(&1 * 2)) == {11, 22}
    end
  end
end
