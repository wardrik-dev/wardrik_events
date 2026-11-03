defmodule WardrikEvents.Sink do
  @moduledoc """
  A behaviour for sending events on, to a warehouse, a queue or tracing.

      defmodule MyApp.Shipper do
        @behaviour WardrikEvents.Sink

        @impl true
        def handle_event(event, opts), do: MyApp.Queue.push(opts[:queue], event)
      end

      {WardrikEvents, name: MyApp.Events, store: store, sinks: [{MyApp.Shipper, queue: :events}]}

  Each sink gets every event once it's kept, agents' and the audit's, with its `"sequence"` in its
  log. Live events, like `wardrik.message.delta`, aren't kept, so they don't come.

    * It runs in the process that wrote the event, often the agent's, so return quickly.
    * An event whose node stops before the sink takes it isn't sent. Each agent's log still holds
      it: read on with `WardrikEvents.page/3` from the last `"sequence"` your sink saw.
  """

  @doc "Handles one kept event. The return value is ignored."
  @callback handle_event(WardrikEvents.CloudEvent.t(), opts :: term()) :: any()

  @doc false
  @spec deliver([module() | {module(), term()}], WardrikEvents.CloudEvent.t()) :: :ok
  def deliver(sinks, event) do
    for sink <- sinks do
      {module, opts} = entry(sink)
      module.handle_event(event, opts)
    end

    :ok
  end

  defp entry({module, opts}), do: {module, opts}
  defp entry(module), do: {module, []}
end
