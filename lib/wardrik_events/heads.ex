defmodule WardrikEvents.Heads do
  @moduledoc "Holds the events' settings and each log's last sequence number, in a table its process owns."

  use GenServer

  @doc false
  def start_link(settings), do: GenServer.start_link(__MODULE__, settings, name: settings.heads)

  @doc "Returns the settings of the events whose table is `heads`."
  @spec settings(atom()) :: map()
  def settings(heads) do
    [{:settings, settings}] = :ets.lookup(heads, :settings)
    settings
  end

  @impl GenServer
  def init(settings) do
    table = :ets.new(settings.heads, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(table, {:settings, settings})
    {:ok, table}
  end
end
