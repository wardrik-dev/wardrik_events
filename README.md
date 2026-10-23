# WardrikEvents

Each agent's events as CloudEvents: a log to read back, and a stream to follow.

## Install

```elixir
def deps do
  [
    {:wardrik_events, "~> 0.1"}
  ]
end
```

## Start it

Add `WardrikEvents` to your supervision tree, after a [WardrikStore](https://hexdocs.pm/wardrik_store)
to keep the logs in, and give it to each [WardrikCore](https://hexdocs.pm/wardrik_core) agent:

```elixir
store = WardrikStore.ETS.new(name: MyApp.Store)

children = [
  {WardrikStore, store},
  {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
]

WardrikCore.start_agent(
  agent_id: "acme/triage",
  model: "anthropic:claude-haiku-4-5",
  events: {WardrikEvents, MyApp.Events}
)
```

| Key | Takes | Unless set |
| --- | --- | --- |
| `name` | the name you read the events by, and the only one you need | required |
| `store` | the `WardrikStore` to keep the logs in | required |
| `deployment` | the deployment's name, in each event's `source` | `"local"` |
| `retention` | the days events are kept, or `:infinity` | 30 |

`WardrikStore.ETS` keeps events in memory, for development and tests. To keep them on disk, use
`WardrikStore.EKV`, with the `ekv` dependency. Every function takes the name you gave, and raises
saying so where it isn't started.

## What an agent writes

Each agent writes one log, named by its id. Each event in it is a
[CloudEvent](https://cloudevents.io) of [WardrikProtocol](https://hexdocs.pm/wardrik_protocol):

```elixir
%{
  "specversion" => "1.0",
  "id" => "019a0b6e-6f1c-7c3a-9d2e-4b1f8a2c5d70",
  "type" => "wardrik.run.started",
  "source" => "wardrik://acme-prod/agents/acme/triage",
  "subject" => "agents/acme/triage",
  "time" => "2026-10-22T09:00:00.000000Z",
  "sequence" => "000000000007",
  "traceparent" => "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
  "data" => %{"run" => %{"id" => "0af7651916cd43dd8448eb211c80319c", "message_ids" => ["msg_1"]}}
}
```

`sequence` counts up in each log. A run's events share its trace, so `traceparent` ties them
together.

Some events are live: `message.delta`, `reasoning.delta`, `tool_call.progress` and
`activity.changed`. They reach whoever is following, and are never kept, so they carry no
`sequence`. The event that completes them is kept.

## Read a log back

Name the logs with patterns, where `*` stands for any run of characters and `?` for one:

```elixir
{:ok, %{events: events, next: next}} = WardrikEvents.page(MyApp.Events, ["acme/*"], limit: 100)
{:ok, %{events: more}} = WardrikEvents.page(MyApp.Events, ["acme/*"], after: next)
```

Across several logs, events come oldest first. `next` is a cursor, the text to pass as `:after` to
read on. `head/2` is the cursor at the end of the logs, to read only what comes after now.

Events older than the retention are dropped, hourly. A reader whose cursor points into what was
dropped gets a `wardrik.events.dropped` first, saying how many it missed, then reads on.

## Follow it

```elixir
{:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/*"], after: cursor)

Enum.each(stream, fn {event, cursor} ->
  show(event)
  remember(WardrikEvents.Cursor.encode(cursor))
end)
```

The stream reads on from `:after`, or from what's written next without one, and follows the logs
for as long as you take from it, live events too. Pass `:types`, like `["wardrik.run.completed"]`,
to take only those. A `wardrik.events.dropped` comes whatever the types.

Notice you never miss an event or see one twice between reading back and following. The stream
subscribes before it reads back, and drops what it already handed over.

## Follow it into your app

A listener hands each event to your module, with the cursor past it, as text. Save the cursor with
what you did, and it carries on from there after a restart:

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

children = [{MyApp.Runs, events: MyApp.Events, types: ["wardrik.run.completed"]}]
```

| Key | Takes | Unless set |
| --- | --- | --- |
| `events` | the name of your `WardrikEvents` | required |
| `resources` | the patterns of the logs to follow | `["*"]`, every agent |
| `types` | the event types to take | every type |
| `after` | the cursor to start after, when you don't define `cursor/1` | what's written next |

Each event is handed over at least once. A `handle/2` that raises or returns anything but `:ok`
restarts the listener from the cursor you saved, so drop a repeat by the event's `id`.

## What an agent is doing now

```elixir
WardrikEvents.activity(MyApp.Events, "acme/triage")
#=> %{
#=>   "doing" => [%{"type" => "wardrik.tool_call.started", "tool" => %{"name" => "github__search_issues", "call" => %{"id" => "call_1"}}}],
#=>   "since" => "2026-10-22T09:00:03.000000Z",
#=>   "pending" => 1
#=> }
```

`doing` is every tool call running, oldest first, or else the last thing the agent started, like a
model call or a wait on a person. It's empty once a run ends. `pending` counts the messages waiting
behind it. Each change is an `activity.changed` to whoever follows the agent's log.

## Views

A view folds a log, oldest first, into what a surface shows of it. `view/4` folds every event a
log keeps, and returns the cursor after them, to follow on from:

```elixir
{timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)
WardrikEvents.Timeline.items(timeline)
```

| View | Shows |
| --- | --- |
| `WardrikEvents.Timeline` | the conversation: each message someone sent, and each run that answered it, step by step |
| `WardrikEvents.Runs` | each run, with who started it, how long it took, its tool calls, tokens and cost, and the totals |
| `WardrikEvents.Activity` | what the agent is doing now, as `activity/2` returns it |

To keep a view current, fold each event the stream brings into the same state. Write your own
with the `WardrikEvents.View` behaviour.

## Write your own events

A host writes what it does to an agent into the agent's log, as core's events are written: kept,
sent to subscribers, and folded into its activity. Each is a type of the protocol's:

```elixir
data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}
:ok = WardrikEvents.append(MyApp.Events, "acme/digest", {"trigger.fired", "triggers/daily", data})
```

## The audit

The audit is one log for the deployment, of what people changed and who changed it:

```elixir
change = {"agent.forgotten", "agents/acme/triage", %{"agent" => %{"id" => "acme/triage"}}}
:ok = WardrikEvents.record(MyApp.Events, change, actor: principal)

%{events: changes, next: next} = WardrikEvents.audit(MyApp.Events, limit: 100)
```

`:actor` is the `WardrikCore.Principal` who made the change, and `:assumed_by` who acted for them,
first to last. The audit is kept apart from the agents' logs, and is dropped with the same
retention.

## License

Apache-2.0
