defmodule WardrikEvents.Log do
  @moduledoc """
  Reads and writes one log: an agent's kept events in a store, in order, each under its sequence
  number.

  An index lists every log.
  """

  alias WardrikEvents.CloudEvent

  @digits 12
  @index "logs/"
  @batch 100

  @doc "Writes `event` at the log's next sequence number. Returns the event with its `sequence`."
  @spec append(WardrikStore.t(), :ets.table(), String.t(), CloudEvent.t()) ::
          {:ok, CloudEvent.t()} | {:error, term()}
  def append(store, heads, log, event), do: append(store, heads, log, event, 1)

  @doc "Returns the sequence number of the log's last event. If every event was dropped, returns the last dropped one's."
  @spec last(WardrikStore.t(), String.t()) :: non_neg_integer()
  def last(store, log), do: gallop(store, log, oldest(store, log) - 1, 1)

  @doc "Returns the oldest sequence number the log holds. If it holds none, returns the next one it will write."
  @spec oldest(WardrikStore.t(), String.t()) :: pos_integer()
  def oldest(store, log) do
    case WardrikStore.get(store, index_key(log)) do
      %{"dropped" => dropped} -> dropped + 1
      nil -> 1
    end
  end

  @doc """
  Returns up to `limit` events after the sequence number `after_sequence`.

  Returns `{:dropped, oldest}` if retention dropped the next one.
  """
  @spec read(WardrikStore.t(), String.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [CloudEvent.t()]} | {:dropped, pos_integer()}
  def read(store, log, after_sequence, limit) do
    oldest = oldest(store, log)

    if after_sequence + 1 < oldest,
      do: {:dropped, oldest},
      else: {:ok, taken(store, log, after_sequence + 1, limit)}
  end

  @doc "Returns the logs whose names start with `prefix`."
  @spec logs(WardrikStore.t(), String.t()) :: [String.t()]
  def logs(store, prefix \\ "") do
    for key <- WardrikStore.keys(store, index_key(prefix)),
        do: key |> String.replace_prefix(@index, "") |> URI.decode_www_form()
  end

  @doc "Drops the log's events written before `cutoff`."
  @spec trim(WardrikStore.t(), String.t(), DateTime.t()) :: :ok | {:error, term()}
  def trim(store, log, cutoff) do
    {dropped, vsn} = lookup_dropped(store, log)

    case expired_through(store, log, dropped, cutoff) do
      ^dropped -> :ok
      last -> drop(store, log, dropped + 1, last, vsn)
    end
  end

  defp append(store, heads, log, event, retries) do
    with {:ok, sequence} <- next(store, heads, log) do
      sequenced = CloudEvent.sequenced(event, sequence)

      case WardrikStore.put(store, key(log, sequence), sequenced, if_vsn: nil) do
        {:ok, _vsn} ->
          {:ok, sequenced}

        {:error, reason} ->
          :ets.delete(heads, head_key(store, log))
          retried(reason, store, heads, log, event, retries)
      end
    end
  end

  defp retried(:conflict, store, heads, log, event, retries) when retries > 0,
    do: append(store, heads, log, event, retries - 1)

  defp retried(reason, _store, _heads, _log, _event, _retries), do: {:error, reason}

  defp next(store, heads, log) do
    with :ok <- loaded(store, heads, log),
         do: {:ok, :ets.update_counter(heads, head_key(store, log), 1)}
  end

  defp loaded(store, heads, log) do
    if :ets.member(heads, head_key(store, log)), do: :ok, else: load(store, heads, log)
  end

  defp load(store, heads, log) do
    with :ok <- indexed(store, log) do
      :ets.insert_new(heads, {head_key(store, log), last(store, log)})
      :ok
    end
  end

  defp head_key(store, log), do: {store.scope, log}

  defp indexed(store, log) do
    case WardrikStore.put(store, index_key(log), %{"dropped" => 0}, if_vsn: nil) do
      {:ok, _vsn} -> :ok
      {:error, :conflict} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp taken(store, log, first, limit) do
    keys = for sequence <- first..(first + limit - 1)//1, do: key(log, sequence)
    found = WardrikStore.lookup_many(store, keys)

    keys
    |> Enum.map(&Map.get(found, &1))
    |> Enum.take_while(&(&1 != nil))
    |> Enum.map(fn {event, _vsn} -> event end)
  end

  defp gallop(store, log, known, step) do
    if written?(store, log, known + step),
      do: gallop(store, log, known + step, step * 2),
      else: bisect(store, log, known, known + step)
  end

  defp bisect(_store, _log, known, missing) when missing - known <= 1, do: known

  defp bisect(store, log, known, missing) do
    middle = div(known + missing, 2)

    if written?(store, log, middle),
      do: bisect(store, log, middle, missing),
      else: bisect(store, log, known, middle)
  end

  defp written?(store, log, sequence), do: WardrikStore.get(store, key(log, sequence)) != nil

  defp lookup_dropped(store, log) do
    case WardrikStore.lookup(store, index_key(log)) do
      {%{"dropped" => dropped}, vsn} -> {dropped, vsn}
      nil -> {0, nil}
    end
  end

  defp expired_through(store, log, sequence, cutoff) do
    expired = store |> taken(log, sequence + 1, @batch) |> Enum.take_while(&expired?(&1, cutoff))
    through = sequence + length(expired)

    if length(expired) == @batch,
      do: expired_through(store, log, through, cutoff),
      else: through
  end

  defp expired?(%{"time" => time}, cutoff) do
    case DateTime.from_iso8601(time) do
      {:ok, at, _offset} -> DateTime.compare(at, cutoff) == :lt
      _unreadable -> false
    end
  end

  defp expired?(_event, _cutoff), do: false

  defp drop(store, log, first, last, vsn) do
    with {:ok, _vsn} <- WardrikStore.put(store, index_key(log), %{"dropped" => last}, if_vsn: vsn) do
      first..last//1
      |> Enum.map(&WardrikStore.delete(store, key(log, &1)))
      |> Enum.find(:ok, &match?({:error, _reason}, &1))
    end
  end

  defp key(log, sequence),
    do: prefix(log) <> (sequence |> Integer.to_string() |> String.pad_leading(@digits, "0"))

  defp index_key(log), do: @index <> URI.encode_www_form(log)

  defp prefix(log), do: "events/" <> URI.encode_www_form(log) <> "/"
end
