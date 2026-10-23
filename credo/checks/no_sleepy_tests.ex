defmodule WardrikEvents.Credo.Check.NoSleepyTests do
  use Credo.Check,
    base_priority: :high,
    category: :warning,
    param_defaults: [max_sleep: 0],
    explanations: [
      check: """
      Tests should not sleep.

      A `Process.sleep/1` in a test is a guess about how long something else
      will take. It is slow when the guess is too high, and flaky when it is
      too low — and the guess that passes on a fast laptop is the one that
      fails in CI.

      Coordinate with messages instead. The concurrent thing you are waiting on
      can almost always tell you it happened:

          # sleepy — hopes the turn is still running
          Agent.ask(agent, "first")
          Process.sleep(20)
          Agent.ask(agent, "second")

          # deterministic — the turn says it started, and blocks until released
          Agent.ask(agent, "first")
          assert_receive {:turn, _context, turn}
          Agent.ask(agent, "second")
          send(turn, :release)

      The usual replacements:

        * waiting for an async result — publish an event and `assert_receive` it
        * waiting for a cast to land — a `GenServer.call/2` is a barrier, since
          it can only be handled after every cast you sent before it
        * holding an operation open — have it block on a message you send
        * forcing an out-of-order completion — release the slow one last, rather
          than sleeping different amounts

      If a sleep is genuinely unavoidable, say why in a comment and exclude the
      file explicitly rather than leaving it to be copied.
      """,
      params: [
        max_sleep: "Milliseconds a sleep may last before it is reported (0 reports all)."
      ]
    ]

  @doc false
  @impl true
  def run(%SourceFile{} = source_file, params) do
    ctx = Context.build(source_file, params, __MODULE__)

    result =
      Credo.Code.prewalk(
        source_file,
        &walk(&1, &2, Params.get(params, :max_sleep, __MODULE__)),
        ctx
      )

    result.issues
  end

  # Process.sleep(…)
  defp walk({{:., _, [{:__aliases__, meta, [:Process]}, :sleep]}, _, args} = ast, ctx, max) do
    {ast, maybe_issue(ctx, meta, "Process.sleep", args, max)}
  end

  defp walk(
         {{:., _, [{:__aliases__, meta, [:"Elixir", :Process]}, :sleep]}, _, args} = ast,
         ctx,
         max
       ) do
    {ast, maybe_issue(ctx, meta, "Process.sleep", args, max)}
  end

  # :timer.sleep(…)
  defp walk({{:., meta, [:timer, :sleep]}, _, args} = ast, ctx, max) do
    {ast, maybe_issue(ctx, meta, ":timer.sleep", args, max)}
  end

  defp walk(ast, ctx, _max), do: {ast, ctx}

  defp maybe_issue(ctx, meta, trigger, args, max_sleep) do
    if reportable?(args, max_sleep) do
      Context.put_issue(ctx, issue_for(ctx, meta, trigger))
    else
      ctx
    end
  end

  # A literal shorter than :max_sleep is tolerated; anything computed is not,
  # since its duration cannot be read off the source.
  defp reportable?([duration], max_sleep) when is_integer(duration), do: duration > max_sleep
  defp reportable?(_args, _max_sleep), do: true

  defp issue_for(issue_meta, meta, trigger) do
    format_issue(
      issue_meta,
      message: "Tests should not sleep; coordinate with messages instead.",
      trigger: trigger,
      line_no: meta[:line],
      column: meta[:column]
    )
  end
end
