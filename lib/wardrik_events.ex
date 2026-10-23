defmodule WardrikEvents do
  @moduledoc """
  Each agent's events as CloudEvents: a log to read back, and a stream to follow.

      children = [
        {WardrikStore, store},
        {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
      ]

      WardrikCore.start_agent(agent_id: "acme/triage", model: "...", events: {WardrikEvents, MyApp.Events})
  """

  @behaviour WardrikCore.Events

  use Supervisor

  require Logger

  alias WardrikEvents.Cursor
  alias WardrikEvents.Encoder
  alias WardrikEvents.Log

  @default_limit 100
  @max_limit 500

  @doc "Starts the events. Takes `:name`, `:store` and `:deployment`."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :store, deployment: "local"])
    Supervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl WardrikCore.Events
  def handle_event(event, events) do
    settings = settings(events)

    for cloud_event <- Encoder.encode(event, settings.deployment),
        do: publish(settings, event.metadata.agent_id, cloud_event)

    :ok
  end

  @doc "Joins the calling process to `log`'s stream. It's sent `{WardrikEvents, log, event}` for each event."
  @spec join(atom(), String.t()) :: :ok
  def join(events, log), do: Group.join(settings(events).group, topic(log))

  @doc "Returns the cursor at the end of every log the `resources` patterns cover."
  @spec head(atom(), [String.t()]) :: String.t()
  def head(events, resources) do
    %{store: store} = settings(events)

    store
    |> covered(resources)
    |> Map.new(fn log -> {log, Log.last(store, log)} end)
    |> Cursor.encode()
  end

  @doc """
  Reads a page of kept events from the logs the `resources` patterns cover, oldest first.

  Takes `:after`, a cursor, and `:limit`, 100 unless set and at most 500. Returns the events and
  `next`, the cursor after them.
  """
  @spec page(atom(), [String.t()], keyword()) ::
          {:ok, %{events: [map()], next: String.t()}} | {:error, :invalid_cursor}
  def page(events, resources, opts \\ []) do
    %{store: store} = settings(events)
    limit = opts |> Keyword.get(:limit, @default_limit) |> min(@max_limit)

    with {:ok, cursor} <- after_cursor(opts[:after]) do
      read =
        store
        |> covered(resources)
        |> Enum.map(fn log -> read(store, log, Map.get(cursor, log, 0), limit) end)
        |> merged(limit)

      {:ok, %{events: Enum.map(read, &elem(&1, 1)), next: Cursor.encode(advanced(cursor, read))}}
    end
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    settings = %{
      store: opts |> Keyword.fetch!(:store) |> WardrikStore.scope("wardrik_events"),
      deployment: Keyword.fetch!(opts, :deployment),
      heads: Module.concat(name, Heads),
      group: Module.concat(name, Group)
    }

    children = [
      {WardrikEvents.Heads, settings},
      {Group, name: settings.group, log: false}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp publish(settings, log, cloud_event) do
    if WardrikProtocol.live?(cloud_event["type"]),
      do: broadcast(settings, log, cloud_event),
      else: kept(settings, log, cloud_event)
  end

  defp kept(settings, log, cloud_event) do
    case Log.append(settings.store, settings.heads, log, cloud_event) do
      {:ok, sequenced} ->
        broadcast(settings, log, sequenced)

      {:error, reason} ->
        Logger.error("Couldn't keep #{cloud_event["type"]} for #{log}: #{inspect(reason)}")
    end
  end

  defp broadcast(settings, log, cloud_event),
    do: Group.dispatch(settings.group, topic(log), {__MODULE__, log, cloud_event})

  defp settings(events), do: WardrikEvents.Heads.settings(Module.concat(events, Heads))

  defp topic(log), do: "log:" <> log

  defp covered(store, resources) do
    patterns = Enum.map(resources, &pattern/1)
    Enum.filter(Log.logs(store), fn log -> Enum.any?(patterns, &Regex.match?(&1, log)) end)
  end

  defp pattern(resource) do
    resource
    |> Regex.escape()
    |> String.replace("\\*", ".*")
    |> String.replace("\\?", ".")
    |> then(&Regex.compile!("^" <> &1 <> "$"))
  end

  defp after_cursor(nil), do: {:ok, %{}}
  defp after_cursor(text), do: Cursor.decode(text)

  defp read(store, log, after_sequence, limit) do
    for event <- Log.read(store, log, after_sequence, limit), do: {log, event}
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
      log = Enum.min_by(reads, fn [{_log, event} | _rest] -> {event["time"], event["id"]} end)

    {oldest, [rest | List.delete(reads, log)]}
  end

  defp advanced(cursor, read) do
    Enum.reduce(read, cursor, fn {log, event}, cursor ->
      Map.put(cursor, log, String.to_integer(event["sequence"]))
    end)
  end
end
