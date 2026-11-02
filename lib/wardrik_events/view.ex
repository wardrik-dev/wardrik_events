defmodule WardrikEvents.View do
  @moduledoc """
  A behaviour for views. A view folds a log's events, oldest first, into what a screen shows.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "agt_1", WardrikEvents.Timeline)

  To keep it current, fold in new events:

      {:ok, stream} =
        WardrikEvents.stream(MyApp.Events, [{"agt_1", nil}], after: WardrikEvents.Cursor.encode(cursor))

      Enum.reduce(stream, timeline, fn {event, _cursor}, timeline ->
        WardrikEvents.Timeline.handle_event(event, timeline)
      end)
  """

  alias WardrikEvents.CloudEvent

  @doc "Returns the state before any event."
  @callback init(opts :: keyword()) :: term()

  @doc "Folds one event into the state."
  @callback handle_event(CloudEvent.t(), state :: term()) :: term()

  @doc "Folds `events`, oldest first, into a new state of `view`. `opts` go to `c:init/1`."
  @spec fold(Enumerable.t(CloudEvent.t()), module(), keyword()) :: term()
  def fold(events, view, opts \\ []),
    do: Enum.reduce(events, view.init(opts), &view.handle_event/2)
end
