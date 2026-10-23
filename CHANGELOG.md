# Changelog

## 0.1.0 (unreleased)

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
