defmodule WardrikEvents.Heads do
  @moduledoc """
  Holds the events' settings, each log's last sequence number and each agent's activity in an ETS
  table.
  """

  use GenServer

  @doc false
  def start_link(settings), do: GenServer.start_link(__MODULE__, settings, name: settings.heads)

  @doc "Returns the settings kept in the table `heads`."
  @spec settings(atom()) :: map()
  def settings(heads) do
    [{:settings, settings}] = :ets.lookup(heads, :settings)
    settings
  end

  @doc """
  Updates the value under `key` with the function `:with`. Returns `{before, after}`.

      WardrikEvents.Heads.update(heads, :count, initial: 0, with: &(&1 + 1))

  `:with` gets `:initial` if `key` has no value. If another process updates `key` first, it runs
  again on the new value.
  """
  @spec update(atom(), term(), initial: term(), with: (term() -> term())) :: {term(), term()}
  def update(heads, key, args),
    do: updated(heads, key, Keyword.fetch!(args, :initial), Keyword.fetch!(args, :with))

  defp updated(heads, key, initial, fun) do
    {version, value} = current(heads, key, initial)
    updated = fun.(value)

    if swapped?(heads, key, version, updated),
      do: {value, updated},
      else: updated(heads, key, initial, fun)
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
