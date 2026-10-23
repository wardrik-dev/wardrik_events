defmodule WardrikEvents.Log do
  @moduledoc "A log is one agent's kept events, in order, each under its sequence number in a store."

  alias WardrikEvents.CloudEvent

  @digits 12

  @doc "Writes `event` at the log's next sequence number, and returns it with that number."
  @spec append(WardrikStore.t(), :ets.table(), String.t(), CloudEvent.t()) ::
          {:ok, CloudEvent.t()} | {:error, term()}
  def append(store, heads, log, event), do: append(store, heads, log, event, 1)

  @doc "Returns the sequence number of the log's last event, or 0 when it has none."
  @spec last(WardrikStore.t(), String.t()) :: non_neg_integer()
  def last(store, log) do
    store
    |> WardrikStore.keys(prefix(log))
    |> List.last()
    |> sequence()
  end

  @doc "Reads up to `limit` events after the sequence number `after`."
  @spec read(WardrikStore.t(), String.t(), non_neg_integer(), pos_integer()) :: [CloudEvent.t()]
  def read(store, log, after_sequence, limit) do
    store
    |> WardrikStore.keys(prefix(log))
    |> Enum.drop_while(fn key -> sequence(key) <= after_sequence end)
    |> Enum.take(limit)
    |> Enum.map(&WardrikStore.get(store, &1))
  end

  @doc "Lists every log the store holds."
  @spec logs(WardrikStore.t()) :: [String.t()]
  def logs(store) do
    store
    |> WardrikStore.keys()
    |> Enum.map(fn key -> key |> String.split("/", parts: 2) |> hd() |> URI.decode_www_form() end)
    |> Enum.uniq()
  end

  defp append(store, heads, log, event, retries) do
    sequence = next(store, heads, log)
    sequenced = CloudEvent.sequenced(event, sequence)

    case WardrikStore.put(store, key(log, sequence), sequenced, if_vsn: nil) do
      {:ok, _vsn} ->
        {:ok, sequenced}

      {:error, reason} ->
        :ets.delete(heads, log)
        retried(reason, store, heads, log, event, retries)
    end
  end

  defp retried(:conflict, store, heads, log, event, retries) when retries > 0,
    do: append(store, heads, log, event, retries - 1)

  defp retried(reason, _store, _heads, _log, _event, _retries), do: {:error, reason}

  defp next(store, heads, log) do
    if not :ets.member(heads, log), do: :ets.insert_new(heads, {log, last(store, log)})
    :ets.update_counter(heads, log, 1)
  end

  defp key(log, sequence),
    do: prefix(log) <> (sequence |> Integer.to_string() |> String.pad_leading(@digits, "0"))

  defp prefix(log), do: URI.encode_www_form(log) <> "/"

  defp sequence(nil), do: 0
  defp sequence(key), do: key |> String.split("/") |> List.last() |> String.to_integer()
end
