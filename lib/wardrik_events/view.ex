defmodule WardrikEvents.View do
  @moduledoc """
  The behaviour for views. A view folds an agent's events, oldest first, into what a screen shows.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)

  To keep a view current, stream the log after `cursor` and fold each event into the same state:

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/triage"], after: WardrikEvents.Cursor.encode(cursor))

      Enum.reduce(stream, timeline, fn {event, _cursor}, timeline ->
        WardrikEvents.Timeline.handle_event(event, timeline)
      end)

  See `WardrikEvents.Timeline`, `WardrikEvents.Runs` and `WardrikEvents.Activity`.
  """

  alias WardrikEvents.CloudEvent

  @doc "Returns the state before any event."
  @callback init(opts :: keyword()) :: term()

  @doc "Folds one event into the state."
  @callback handle_event(CloudEvent.t(), state :: term()) :: term()

  @doc "Folds `events`, oldest first, into a new state of `view`. `opts` are passed to `c:init/1`."
  @spec fold(Enumerable.t(CloudEvent.t()), module(), keyword()) :: term()
  def fold(events, view, opts \\ []),
    do: Enum.reduce(events, view.init(opts), &view.handle_event/2)
end
