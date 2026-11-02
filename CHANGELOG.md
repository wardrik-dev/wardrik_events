# Changelog

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
