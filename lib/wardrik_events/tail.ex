defmodule WardrikEvents.Tail do
  @moduledoc """
  A tail reads the logs a reader's patterns cover from its cursor, then follows them as they're
  written, taking each kept event once and in order.
  """

  alias WardrikEvents.Cursor
  alias WardrikEvents.Reader
  alias WardrikEvents.Subscribers

  @page 500

  @doc "Streams `{event, cursor}` from after `cursor`, or from what's written next when it's `nil`."
  @spec stream(map(), [String.t()], Cursor.t() | nil, [String.t()] | :all) :: Enumerable.t()
  def stream(settings, resources, cursor, types) do
    fn -> open(settings, resources, cursor) end
    |> Stream.resource(&next/1, &close/1)
    |> Stream.filter(fn {event, _cursor} -> takes?(types, event["type"]) end)
  end

  defp open(settings, resources, cursor) do
    :ok = Subscribers.subscribe(settings.group, resources)
    {replay, positions} = start(settings.store, resources, cursor)

    %{
      settings: settings,
      resources: resources,
      replay: replay,
      positions: positions
    }
  end

  defp start(store, resources, nil), do: {[], Reader.head(store, resources)}
  defp start(store, resources, cursor), do: {Reader.covered(store, resources), cursor}

  defp next(%{replay: [log | logs]} = tail) do
    case read(tail, log) do
      {[], tail} -> next(%{tail | replay: logs})
      read -> read
    end
  end

  defp next(%{replay: []} = tail) do
    receive do
      {WardrikEvents, log, event} -> arrived(tail, log, event)
    end
  end

  defp arrived(tail, log, %{"sequence" => sequence}) do
    if String.to_integer(sequence) > Map.get(tail.positions, log, 0),
      do: caught_up(tail, log, []),
      else: {[], tail}
  end

  defp arrived(tail, _log, live), do: {[{live, tail.positions}], tail}

  defp caught_up(tail, log, taken) do
    case read(tail, log) do
      {[], tail} -> {taken, tail}
      {read, tail} -> caught_up(tail, log, taken ++ read)
    end
  end

  defp read(tail, log) do
    tail.settings
    |> Reader.entries(log, Map.get(tail.positions, log, 0), @page)
    |> Enum.map_reduce(tail, &passed/2)
  end

  defp passed({_log, _sequence, event} = entry, tail) do
    positions = Reader.advanced(tail.positions, [entry])
    {{event, positions}, %{tail | positions: positions}}
  end

  defp close(tail) do
    :ok = Subscribers.unsubscribe(tail.settings.group, tail.resources)
    flush()
  end

  defp flush do
    receive do
      {WardrikEvents, _log, _event} -> flush()
    after
      0 -> :ok
    end
  end

  defp takes?(_types, "wardrik.events.dropped"), do: true
  defp takes?(:all, _type), do: true
  defp takes?(types, type), do: type in types
end
