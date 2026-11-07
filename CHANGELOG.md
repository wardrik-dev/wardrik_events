# Changelog

## 0.1.0-rc.6 (2026-11-06)

- Requires `wardrik_protocol` 0.1.0-rc.5.
- `run.interrupted` interrupts carry `arguments`. `name` and `arguments` are the call a person decides on: the call a policy asked about, if it differs from the call that parked.

## 0.1.0-rc.5 (2026-11-05)

- An event or audit change is written however many writers take the next number first. It gave
  up after one, so nodes writing to the audit at once failed.

## 0.1.0-rc.4 (2026-11-05)

- `activity/2` is right on every node. Activity is kept with the last sequence it folded, and
  events another node wrote, as when an agent moves or a trigger fires elsewhere, are read from
  the log first. A read costs one store lookup more.
- Requires wardrik_core, wardrik_store and wardrik_protocol 0.1.0-rc.4.

## 0.1.0-rc.3 (2026-11-03)

### Views

- `Timeline` folds what a delegate says and does into a `:subagent` step of its own, never into
  the run's answer, and counts its cost toward the run.
- `Timeline.touched/1` returns `:reset` where the conversation was cleared.
- `Timeline.item/2`, `current_run/1`, `running?/1` and `interrupts/1`.
- `Runs` keeps each run's events with `events: true`. `Runs.sum/1` adds totals up.
- `CloudEvent.span_id/1`.

### Breaking

- `page/3`, `head/2` and `stream/3` read only the agents and logs named. `:all` is gone from them;
  `subscribe/2` still takes it, for every agent's events as they happen.
- `WardrikEvents.Listener` takes `:resources`, required: it follows named agents only. To take
  every agent's events, give the events a sink.

### Added

- `WardrikEvents.Sink`, and the `:sinks` option: each gets every kept event, agents' and the
  audit's, with its sequence.

### Fixed

- `activity/2` survives a restart: an agent's activity is folded again from its log, so a run
  waiting on a person still shows as waiting.

## 0.1.0-rc.2 (2026-11-02)

### Breaking

- Each conversation has its own log, `{agent, conversation}`. An agent ID is never parsed.
- `append/4`, `activity/2` and `view/4` take an agent ID for its main conversation, or
  `{agent, conversation}` for another: `WardrikEvents.activity(MyApp.Events, "agt_1")`.
- `WardrikEvents.Pattern`, with `*` and `?`, is gone. Reading functions take a list of agent IDs,
  each with all its conversations, and `{agent, conversation}` logs, or `:all`. `covers?/2` says
  whether one covers a log.
- Events carry `agent` and, for a conversation, `conversation` attributes. `source` URL-encodes
  the agent ID. `agent.started` carries `conversation` only for a conversation.
- `CloudEvent.new/2` takes its arguments by name, with `log:` in place of `agent_id:`.
- Cursors encode a list of `[agent, conversation, sequence]`. rc.1 cursors don't decode.
- `WardrikEvents.Listener` follows `:all` unless `:resources` is set.
- Subscribers receive `{WardrikEvents, {agent, conversation}, event}`.
- Internal modules, such as `Log`, `Reader`, `Tail` and `Heads`, are hidden from the docs.

## 0.1.0-rc.1 (2026-10-30)

- Each WardrikCore agent's events, encoded as the CloudEvents of WardrikProtocol, and kept in one
  log per agent in a WardrikStore.
- `page/3` reads the logs back by pattern, oldest first, with cursors to read on from.
- `stream/3` reads back from a cursor and follows on, live events too, with nothing missed or
  repeated in between.
- Retention that drops events past their age, and a `wardrik.events.dropped` for readers who
  missed them.
- `WardrikEvents.Listener`, which follows the logs into your app at least once, from the cursor you
  saved.
- `activity/2`, what each agent is doing now, published as `activity.changed`.
- `WardrikEvents.View`, with `Timeline`, `Runs` and `Activity` views, and `view/4` to fold a log.
- The audit: `record/3` writes who changed what, and `audit/2` pages it.
