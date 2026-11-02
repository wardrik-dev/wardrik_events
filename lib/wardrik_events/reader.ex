defmodule WardrikEvents.Reader do
  @moduledoc false

  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Cursor
  alias WardrikEvents.Log

  @chunk 500

  @typedoc "`{log, sequence, event}`."
  @type entry() :: {WardrikEvents.log(), non_neg_integer(), CloudEvent.t()}

  @doc "Returns the kept logs `resources` cover."
  @spec covered(WardrikStore.t(), WardrikEvents.resources()) :: [WardrikEvents.log()]
  def covered(store, :all), do: Log.logs(store)

  def covered(store, resources) do
    resources
    |> Enum.flat_map(&logs(store, &1))
    |> Enum.uniq()
  end

  defp logs(store, {agent, _conversation} = log),
    do: store |> Log.logs(agent) |> Enum.filter(&(&1 == log))

  defp logs(store, agent), do: Log.logs(store, agent)

  @doc "Returns the cursor at the end of the logs `resources` cover."
  @spec head(WardrikStore.t(), WardrikEvents.resources()) :: Cursor.t()
  def head(store, resources),
    do: store |> covered(resources) |> Map.new(fn log -> {log, Log.last(store, log)} end)

  @doc """
  Returns up to `:limit` entries after the cursor `:after` from the logs `resources` cover,
  oldest first, and the cursor after them.

      WardrikEvents.Reader.page(settings, ["agt_1"], after: %{}, limit: 100)
  """
  @spec page(map(), WardrikEvents.resources(), after: Cursor.t(), limit: pos_integer()) ::
          {[entry()], Cursor.t()}
  def page(settings, resources, args) do
    cursor = Keyword.fetch!(args, :after)
    limit = Keyword.fetch!(args, :limit)

    read =
      settings.store
      |> covered(resources)
      |> Enum.map(&entries(settings, &1, after: Map.get(cursor, &1, 0), limit: limit))
      |> merged(limit)

    {read, advanced(cursor, read)}
  end

  @doc """
  Returns up to `:limit` entries of `log` after the sequence number `:after`.

      WardrikEvents.Reader.entries(settings, {"agt_1", nil}, after: 0, limit: 100)

  If retention dropped the next one, the entries start with a `wardrik.events.dropped`.
  """
  @spec entries(map(), WardrikEvents.log(), after: non_neg_integer(), limit: pos_integer()) ::
          [entry()]
  def entries(settings, log, args) do
    at = Keyword.fetch!(args, :after)
    limit = Keyword.fetch!(args, :limit)

    case Log.read(settings.store, log, after: at, limit: limit) do
      {:ok, events} ->
        numbered(log, events)

      {:dropped, oldest} ->
        {:ok, events} = Log.read(settings.store, log, after: oldest - 1, limit: limit - 1)
        [{log, oldest - 1, dropped(settings, log, at, oldest, events)} | numbered(log, events)]
    end
  end

  @doc "Streams every entry of `log`, oldest first."
  @spec all(map(), WardrikEvents.log()) :: Enumerable.t(entry())
  def all(settings, log) do
    0
    |> Stream.unfold(&chunk(settings, log, &1))
    |> Stream.concat()
  end

  @doc "Returns `cursor` moved past each of `entries`."
  @spec advanced(Cursor.t(), [entry()]) :: Cursor.t()
  def advanced(cursor, entries) do
    Enum.reduce(entries, cursor, fn {log, sequence, _event}, cursor ->
      Map.put(cursor, log, sequence)
    end)
  end

  defp chunk(settings, log, at) do
    case entries(settings, log, after: at, limit: @chunk) do
      [] -> nil
      entries -> {entries, last_sequence(entries)}
    end
  end

  defp last_sequence(entries) do
    {_log, sequence, _event} = List.last(entries)
    sequence
  end

  defp numbered(log, events),
    do: for(event <- events, do: {log, String.to_integer(event["sequence"]), event})

  defp dropped(settings, {agent, _conversation} = log, at, oldest, events) do
    about =
      case settings do
        %{source: source, subject: subject} -> [source: source, subject: subject]
        _agents -> [log: log, subject: CloudEvent.subject(agent)]
      end

    CloudEvent.new(
      "events.dropped",
      [
        data: %{"after" => at, "oldest" => oldest},
        deployment: settings.deployment,
        time: resumed_at(events)
      ] ++ about
    )
  end

  defp resumed_at([]), do: DateTime.utc_now()

  defp resumed_at([%{"time" => time} | _events]) do
    {:ok, at, _offset} = DateTime.from_iso8601(time)
    at
  end

  defp merged(reads, limit), do: reads |> Stream.unfold(&next_oldest/1) |> Enum.take(limit)

  defp next_oldest(reads) do
    case Enum.reject(reads, &(&1 == [])) do
      [] -> nil
      reads -> take_oldest(reads)
    end
  end

  defp take_oldest(reads) do
    [oldest | rest] =
      log = Enum.min_by(reads, fn [{_log, _sequence, event} | _rest] -> order(event) end)

    {oldest, [rest | List.delete(reads, log)]}
  end

  defp order(event), do: {event["time"], event["id"]}
end
