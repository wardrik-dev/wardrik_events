defmodule WardrikEvents.Retention do
  @moduledoc "Retention drops each log's events once they're older than the days the events keep them."

  use GenServer

  require Logger

  alias WardrikEvents.Log

  @every :timer.hours(1)

  @doc false
  def start_link(settings), do: GenServer.start_link(__MODULE__, settings)

  @doc "Drops the events written before `cutoff` from every log."
  @spec trim(WardrikStore.t(), DateTime.t()) :: :ok
  def trim(store, cutoff) do
    for log <- Log.logs(store), do: trim(store, log, cutoff)
    :ok
  end

  @impl GenServer
  def init(settings), do: {:ok, settings, {:continue, :trim}}

  @impl GenServer
  def handle_continue(:trim, settings) do
    cutoff = DateTime.add(DateTime.utc_now(), -settings.retention, :day)
    trim(settings.store, cutoff)
    trim(settings.audit, cutoff)
    Process.send_after(self(), :trim, @every)
    {:noreply, settings}
  end

  @impl GenServer
  def handle_info(:trim, settings), do: {:noreply, settings, {:continue, :trim}}

  defp trim(store, log, cutoff) do
    with {:error, reason} <- Log.trim(store, log, cutoff),
         do: Logger.warning("Couldn't trim #{log}'s events: #{inspect(reason)}")
  end
end
