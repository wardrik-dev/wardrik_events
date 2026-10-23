defmodule WardrikEvents.Heads do
  @moduledoc """
  Holds the events' settings, each log's last sequence number, and each agent's activity, in a table
  its process owns.
  """

  use GenServer

  @doc false
  def start_link(settings), do: GenServer.start_link(__MODULE__, settings, name: settings.heads)

  @doc "Returns the settings of the events whose table is `heads`."
  @spec settings(atom()) :: map()
  def settings(heads) do
    [{:settings, settings}] = :ets.lookup(heads, :settings)
    settings
  end

  @doc """
  Replaces the value under `key` with `fun` applied to it, or to `initial` when there is none, and
  returns the value before and after. Another process's update in between makes it apply `fun` again.
  """
  @spec update(atom(), term(), term(), (term() -> term())) :: {term(), term()}
  def update(heads, key, initial, fun) do
    {version, value} = current(heads, key, initial)
    updated = fun.(value)

    if swapped?(heads, key, version, updated),
      do: {value, updated},
      else: update(heads, key, initial, fun)
  end

  defp current(heads, key, initial) do
    case :ets.lookup(heads, key) do
      [{^key, version, value}] -> {version, value}
      [] -> {0, initial}
    end
  end

  defp swapped?(heads, key, 0, value), do: :ets.insert_new(heads, {key, 1, value})

  defp swapped?(heads, key, version, value) do
    replaced = {{{:const, key}, version + 1, {:const, value}}}
    :ets.select_replace(heads, [{{key, version, :_}, [], [replaced]}]) == 1
  end

  @impl GenServer
  def init(settings) do
    table = :ets.new(settings.heads, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(table, {:settings, settings})
    {:ok, table}
  end
end
