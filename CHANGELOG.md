# Changelog

## 0.1.0-rc.2

### Breaking

- `WardrikEvents.CloudEvent.new/2`, `Heads.update/3`, `Log.append/4`, `Log.read/3`,
  `Reader.entries/3`, `Reader.page/3` and `Tail.stream/3` take their arguments by name, as
  `subject:`, `data:`, `initial:`, `with:`, `heads:`, `after:`, `limit:` and `types:`.
- A log is `{agent, conversation}`, with `conversation` `nil` for the agent's own. `append/4`,
  `activity/2`, `view/4` and `Log` take a log, not an agent ID.
- `WardrikEvents.Pattern` is gone, and with it `*` and `?`. Functions take resources: agent IDs,
  each covering its own log and its conversations', exact logs, or `:all`. `covers?/2` says
  whether a resource covers a log.
- Cursors map logs to sequences, and encode as a JSON list of `[agent, conversation, sequence]`.
  Earlier cursors don't decode.
- `CloudEvent.new/2` takes `log:` instead of `agent_id:`. Events carry `agent` and, for a
  conversation, `conversation` extension attributes. `source` URL-encodes the agent.
- `agent.started` carries `conversation` only for a conversation.
- `WardrikEvents.Listener` follows `:all` unless `:resources` is set.
- Messages to subscribers are `{WardrikEvents, {agent, conversation}, event}`.

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
