# WardrikEvents

Keeps each agent's events as CloudEvents, in a log you can read back and follow.

## Install

```elixir
def deps do
  [
    {:wardrik_events, "~> 0.1.0-rc.1"}
  ]
end
```

## Start it

Start `WardrikEvents` after a [WardrikStore](https://hexdocs.pm/wardrik_store), which keeps the
logs. Give it to each [WardrikCore](https://hexdocs.pm/wardrik_core) agent:

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

| Option | Takes | Unless set |
| --- | --- | --- |
| `:name` | what you reach the events by | required |
| `:store` | the `WardrikStore` to keep the logs in | required |
| `:deployment` | the deployment's name, in each event's `source` | `"local"` |
| `:retention` | days events are kept, or `:infinity` | `30` |

`WardrikStore.ETS` keeps events in memory, for development and tests. Use `WardrikStore.EKV` to
keep them on disk.

Every function takes the name you gave. It raises if the events aren't started.

## What an agent writes

Each agent writes one log, named by its id. Each event is a [CloudEvent](https://cloudevents.io)
from [WardrikProtocol](https://hexdocs.pm/wardrik_protocol):

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

* `sequence` counts up in each log.
* A run's events share its trace, so `traceparent` ties them together.

Some events are live: `message.delta`, `reasoning.delta`, `tool_call.progress` and
`activity.changed`. They reach whoever is following, but they're never kept, so they have no
`sequence`. The event that completes them is kept.

## Read a log back

Name logs with patterns. `*` matches any run of characters, and `?` matches one:

```elixir
WardrikEvents.page(MyApp.Events, ["acme/*"], limit: 100)
#=> {:ok,
#=>  %{
#=>    events: [
#=>      %{"type" => "wardrik.run.started", "subject" => "runs/0af76519...", "sequence" => "000000000041", ...},
#=>      %{"type" => "wardrik.message.started", ...},
#=>      ...
#=>    ],
#=>    next: "eyJhY21lL3RyaWFnZSI6NDN9"
#=>  }}

WardrikEvents.page(MyApp.Events, ["acme/*"], after: "eyJhY21lL3RyaWFnZSI6NDN9")
```

* Events come oldest first, across every log matched.
* `next` is a cursor. Pass it as `:after` to read on.
* `head/2` returns the cursor at the end of the logs, to read only newer events.

Events older than the retention are dropped hourly. If your cursor points at dropped events, you
get a `wardrik.events.dropped` first, saying how many you missed. Then reading carries on.

## Follow it

```elixir
{:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/*"], after: cursor)

Enum.each(stream, fn {event, cursor} ->
  show(event)
  remember(WardrikEvents.Cursor.encode(cursor))
end)
```

The stream reads on from `:after`. Without it, the stream starts at what's written next. It follows
the logs, live events too, for as long as you take from it.

Pass `:types`, as `["wardrik.run.completed"]`, to take only those. A `wardrik.events.dropped`
always comes through.

You never miss an event or see one twice between reading back and following.

## Follow it into your app

A listener hands each event to your module, with the cursor past it as text. Save the cursor with
your work, and the listener carries on from there after a restart:

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

| Option | Takes | Unless set |
| --- | --- | --- |
| `:events` | the name of your `WardrikEvents` | required |
| `:resources` | patterns of the logs to follow | `["*"]`, every agent |
| `:types` | the event types to take | every type |
| `:after` | the cursor to start after, if you don't define `cursor/1` | what's written next |

Each event is handed over at least once. If `handle/2` raises or returns anything but `:ok`, the
listener restarts from what `cursor/1` returns. Drop a repeat by the event's `id`.

## What an agent is doing now

```elixir
WardrikEvents.activity(MyApp.Events, "acme/triage")
#=> %{
#=>   "doing" => [%{"type" => "wardrik.tool_call.started", "tool" => %{"name" => "github__search_issues", "call" => %{"id" => "call_1"}}}],
#=>   "since" => "2026-10-22T09:00:03.000000Z",
#=>   "pending" => 1
#=> }
```

* `doing` is every tool call running, oldest first. With none running, it's the last thing the
  agent started, like a model call or a wait on a person. It's empty once a run ends.
* `pending` counts the messages waiting for a run.

Each change is sent as an `activity.changed` to whoever follows the agent's log.

## Views

A view folds a log, oldest first, into what a screen shows. `view/4` folds every kept event, and
returns the cursor after them:

```elixir
{timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)
WardrikEvents.Timeline.items(timeline)
```

| View | Shows |
| --- | --- |
| `WardrikEvents.Timeline` | the conversation: each message someone sent, and each run that answered it, step by step |
| `WardrikEvents.Runs` | each run, with who started it, how long it took, its tool calls, tokens and cost, and the totals |
| `WardrikEvents.Activity` | what the agent is doing now, as `activity/2` returns it |

To keep a view current, stream the log after `cursor` and fold each event into the same state.
Write your own view with the `WardrikEvents.View` behaviour.

## Write your own events

A host can write what it does to an agent into the agent's log. The event is kept, sent to
subscribers and folded into the activity, like the agent's own. Its type comes from the protocol:

```elixir
data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}
:ok = WardrikEvents.append(MyApp.Events, "acme/digest", {"trigger.fired", "triggers/daily", data})
```

## The audit

The audit is one log for the deployment. It records what people changed, and who changed it:

```elixir
change = {"agent.forgotten", "agents/acme/triage", %{"agent" => %{"id" => "acme/triage"}}}
:ok = WardrikEvents.record(MyApp.Events, change, actor: principal)

%{events: changes, next: next} = WardrikEvents.audit(MyApp.Events, limit: 100)
```

* `:actor` is the `WardrikCore.Principal` who made the change.
* `:assumed_by` lists who acted for them, first to last.

The audit is kept apart from the agents' logs. It's dropped with the same retention.

## License

Apache-2.0
