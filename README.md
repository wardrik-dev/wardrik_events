# WardrikEvents

Keeps each agent's events as [CloudEvents](https://cloudevents.io), in logs you can read back and
follow.

## Install

```elixir
def deps do
  [
    {:wardrik_events, "~> 0.1.0-rc.6"}
  ]
end
```

## Start the event log

Run in a fresh IEx session:

```elixir
Mix.install([{:wardrik_events, "~> 0.1.0-rc.6"}])

store = WardrikStore.ETS.new(name: MyApp.Store)

children = [
  {WardrikStore, store},
  {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
]

{:ok, _supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
```

* Give it to each [WardrikCore](https://hexdocs.pm/wardrik_core) agent as
  `events: {WardrikEvents, MyApp.Events}`.
* `WardrikStore.ETS` keeps events in memory. Use `WardrikStore.EKV` to keep them on disk.
* See `WardrikEvents.start_link/1` for the options.

## Read a log

```elixir
{:ok, %{events: events, next: next}} = WardrikEvents.page(MyApp.Events, ["agt_1"], limit: 100)
{:ok, %{events: more}} = WardrikEvents.page(MyApp.Events, ["agt_1"], after: next)
```

* `"agt_1"` reads the agent and all its conversations. `{"agt_1", "C1"}` reads one conversation.
* Events come oldest first. `next` is a cursor to read on from.
* Events older than the retention are dropped. A `wardrik.events.dropped` says how many you missed.

An event:

```elixir
%{
  "type" => "wardrik.run.started",
  "agent" => "agt_1",
  "conversation" => "C1",
  "sequence" => "000000000007",
  "data" => %{"run" => %{"id" => "0af7651916cd43dd8448eb211c80319c", "message_ids" => ["msg_1"]}},
  ...
}
```

## Follow it

```elixir
{:ok, stream} = WardrikEvents.stream(MyApp.Events, [{"agt_1", "C1"}], after: next)

Enum.each(stream, fn {event, cursor} ->
  IO.puts(event["type"])
  IO.inspect(WardrikEvents.Cursor.encode(cursor), label: "cursor")
end)
```

* Without `:after`, the stream starts at the next event written.
* It also carries live events, like `wardrik.message.delta`. These are never kept.
* Persisted events are not missed or repeated when switching from replay to live delivery. Live deltas are not replayed.

## Process events in your app

Integration example: implement `MyApp.Cursors`, `MyApp.Runs.row/1`, and the `agent_runs` table in your app.

```elixir
defmodule MyApp.Runs do
  use WardrikEvents.Listener

  @impl true
  def handle(event, listener) do
    MyApp.Repo.transaction(fn ->
      MyApp.Repo.insert_all("agent_runs", [MyApp.Runs.row(event)], on_conflict: :nothing)
      MyApp.Cursors.put!("runs", listener.cursor)
    end)

    :ok
  end

  @impl true
  def cursor(_listener), do: MyApp.Cursors.get("runs")
end

children = [
  {MyApp.Runs, events: MyApp.Events, resources: ["agt_1"], types: ["wardrik.run.completed"]}
]
```

* Save `listener.cursor` with your work. After a restart, the listener reads on from `cursor/1`.
* Each event arrives at least once. Drop repeats by the event's `id`.

## Send every event on

```elixir
defmodule MyApp.Shipper do
  @behaviour WardrikEvents.Sink

  @impl true
  def handle_event(event, opts), do: MyApp.Queue.push(opts[:queue], event)
end

{WardrikEvents, name: MyApp.Events, store: store, sinks: [{MyApp.Shipper, queue: :events}]}
```

* A sink gets each event once it's kept, agents' and the audit's, in the process that wrote it.
  Return quickly.
* An event whose node stops first isn't sent. Read on from each agent's log with `page/3`.
* `WardrikEvents.subscribe(MyApp.Events, :all)` sends every agent's events, live ones too, to the
  calling process as they happen.

## What an agent is doing

```elixir
WardrikEvents.activity(MyApp.Events, "agt_1")

# => %{"doing" => [%{"type" => "wardrik.tool_call.started", ...}], "since" => "...", "pending" => 1}

{timeline, _cursor} = WardrikEvents.view(MyApp.Events, "agt_1", WardrikEvents.Timeline)
WardrikEvents.Timeline.items(timeline)
```

* A bare agent ID means its main conversation. Pass `{"agt_1", "C1"}` for another.
* Views: `WardrikEvents.Timeline`, `WardrikEvents.Runs` and `WardrikEvents.Activity`. Write your
  own with `WardrikEvents.View`.

## Write events

```elixir
data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}
:ok = WardrikEvents.append(MyApp.Events, "agt_1", {"trigger.fired", "triggers/daily", data})

change = {"agent.forgotten", "agents/agt_1", %{"agent" => %{"id" => "agt_1"}}}
:ok = WardrikEvents.record(MyApp.Events, change, actor: %WardrikCore.Principal{id: "user_42"})
%{events: changes} = WardrikEvents.audit(MyApp.Events)
```

* `append/4` writes to an agent's log, like the agent's own events.
* `record/3` writes to the audit: one log of who changed what.

## License

Apache-2.0
