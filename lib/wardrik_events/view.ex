defmodule WardrikEvents.View do
  @moduledoc """
  A view folds an agent's events, oldest first, into what a surface shows of it.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)

  Follow the log after `cursor`, and fold each event that comes into the same state, to keep it
  current.
  """

  alias WardrikEvents.CloudEvent

  @doc "Returns the view's state before any event."
  @callback init(opts :: keyword()) :: term()

  @doc "Folds one event into the view's state."
  @callback handle_event(CloudEvent.t(), state :: term()) :: term()

  @doc "Folds `events`, oldest first, into a new state of `view`."
  @spec fold(Enumerable.t(CloudEvent.t()), module(), keyword()) :: term()
  def fold(events, view, opts \\ []),
    do: Enum.reduce(events, view.init(opts), &view.handle_event/2)
end
