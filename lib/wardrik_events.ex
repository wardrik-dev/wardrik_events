defmodule WardrikEvents do
  @moduledoc """
  Each agent's events as CloudEvents: a log to read back, and a stream to follow.

      children = [
        {WardrikStore, store},
        {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
      ]

      WardrikCore.start_agent(agent_id: "acme/triage", model: "...", events: {WardrikEvents, MyApp.Events})

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/*"])
      Enum.each(stream, fn {event, cursor} -> save(event, WardrikEvents.Cursor.encode(cursor)) end)
  """

  @behaviour WardrikCore.Events

  use Supervisor

  require Logger

  alias WardrikEvents.Activity
  alias WardrikEvents.Audit
  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Cursor
  alias WardrikEvents.Encoder
  alias WardrikEvents.Heads
  alias WardrikEvents.Log
  alias WardrikEvents.Reader
  alias WardrikEvents.Retention
  alias WardrikEvents.Subscribers
  alias WardrikEvents.Tail

  @default_limit 100
  @max_limit 500

  @doc """
  Starts the events. Takes `:name`, `:store`, `:deployment`, and `:retention`, the days events are
  kept: 30 unless set, or `:infinity`.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :store, deployment: "local", retention: 30])
    Supervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl WardrikCore.Events
  def handle_event(event, events) do
    settings = settings(events)

    for cloud_event <- Encoder.encode(event, settings.deployment) do
      publish(settings, event.metadata.agent_id, cloud_event)
      changed(settings, event, cloud_event)
    end

    :ok
  end

  @doc """
  Subscribes the calling process to the logs the `resources` patterns cover. It's sent
  `{WardrikEvents, log, event}` for each event, live or kept.
  """
  @spec subscribe(atom(), [String.t()]) :: :ok
  def subscribe(events, resources), do: Subscribers.subscribe(settings(events).group, resources)

  @doc "Unsubscribes the calling process from the `resources` patterns."
  @spec unsubscribe(atom(), [String.t()]) :: :ok
  def unsubscribe(events, resources),
    do: Subscribers.unsubscribe(settings(events).group, resources)

  @doc "Returns the cursor at the end of every log the `resources` patterns cover."
  @spec head(atom(), [String.t()]) :: String.t()
  def head(events, resources),
    do: settings(events).store |> Reader.head(resources) |> Cursor.encode()

  @doc """
  Reads a page of kept events from the logs the `resources` patterns cover, oldest first.

  Takes `:after`, a cursor, and `:limit`, 100 unless set and at most 500. Returns the events and
  `next`, the cursor after them.
  """
  @spec page(atom(), [String.t()], keyword()) ::
          {:ok, %{events: [map()], next: String.t()}} | {:error, :invalid_cursor}
  def page(events, resources, opts \\ []) do
    limit = opts |> Keyword.get(:limit, @default_limit) |> min(@max_limit)

    with {:ok, cursor} <- after_cursor(opts[:after]) do
      {read, next} = Reader.page(settings(events), resources, cursor, limit)

      {:ok,
       %{events: for({_log, _sequence, event} <- read, do: event), next: Cursor.encode(next)}}
    end
  end

  @doc """
  Streams the events of the logs the `resources` patterns cover, as `{event, cursor}`, until the
  calling process stops taking them. `WardrikEvents.Cursor.encode/1` turns a cursor into the text
  `:after` takes.

  Takes `:after`, a cursor to read on from, or what's written next without one, and `:types`, the
  event types to take. A `wardrik.events.dropped` comes whatever the types.
  """
  @spec stream(atom(), [String.t()], keyword()) ::
          {:ok, Enumerable.t({map(), Cursor.t()})} | {:error, :invalid_cursor}
  def stream(events, resources, opts \\ []) do
    opts = Keyword.validate!(opts, [:after, types: :all])

    with {:ok, cursor} <- stream_cursor(opts[:after]),
         do: {:ok, Tail.stream(settings(events), resources, cursor, opts[:types])}
  end

  @doc "Returns what the agent writing `log` is doing now, as the protocol's `activity`."
  @spec activity(atom(), String.t()) :: map()
  def activity(events, log) do
    case :ets.lookup(settings(events).heads, {:activity, log}) do
      [{_key, _version, activity}] -> activity
      [] -> Activity.init([])
    end
  end

  @doc """
  Folds every event `log` keeps into a new state of `view`, a `WardrikEvents.View`, and returns it
  with the cursor after them.
  """
  @spec view(atom(), String.t(), module(), keyword()) :: {term(), Cursor.t()}
  def view(events, log, view, opts \\ []) do
    events
    |> settings()
    |> Reader.all(log)
    |> Enum.reduce({view.init(opts), %{log => 0}}, &folded(view, &1, &2))
  end

  @doc """
  Records `change`, a `{type, subject, data}` someone made, in the audit. Takes `:actor`, who made
  it, and `:assumed_by`, who acted for them, first to last.

      change = {"agent.forgotten", "agents/acme/triage", %{"agent" => %{"id" => "acme/triage"}}}
      :ok = WardrikEvents.record(MyApp.Events, change, actor: principal)
  """
  @spec record(atom(), {String.t(), String.t(), map()}, keyword()) :: :ok | {:error, term()}
  def record(events, change, opts \\ []) do
    opts = Keyword.validate!(opts, [:actor, :span, assumed_by: []])

    with {:ok, _event} <- Audit.record(settings(events), change, opts), do: :ok
  end

  @doc """
  Reads a page of the audit, oldest first. Takes `:after`, the sequence number to read after, and
  `:limit`, 100 unless set and at most 500. Returns the changes and `next`, the sequence after them.
  """
  @spec audit(atom(), keyword()) :: %{events: [map()], next: non_neg_integer()}
  def audit(events, opts \\ []) do
    limit = opts |> Keyword.get(:limit, @default_limit) |> min(@max_limit)
    {changes, next} = Audit.page(settings(events), Keyword.get(opts, :after, 0), limit)
    %{events: changes, next: next}
  end

  @doc "Drops the events and changes written before `cutoff`."
  @spec trim(atom(), DateTime.t()) :: :ok
  def trim(events, cutoff) do
    settings = settings(events)
    Retention.trim(settings.store, cutoff)
    Retention.trim(settings.audit, cutoff)
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    store = Keyword.fetch!(opts, :store)

    settings = %{
      store: WardrikStore.scope(store, "wardrik_events"),
      audit: WardrikStore.scope(store, "wardrik_audit"),
      deployment: Keyword.fetch!(opts, :deployment),
      retention: Keyword.fetch!(opts, :retention),
      heads: Module.concat(name, Heads),
      group: Module.concat(name, Group)
    }

    children = [
      {Heads, settings},
      {Group, name: settings.group, log: false}
      | retention(settings)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp retention(%{retention: :infinity}), do: []
  defp retention(settings), do: [{Retention, settings}]

  defp publish(settings, log, cloud_event) do
    if WardrikProtocol.live?(cloud_event["type"]),
      do: Subscribers.publish(settings.group, log, cloud_event),
      else: kept(settings, log, cloud_event)
  end

  defp folded(view, {_log, _sequence, event} = entry, {state, cursor}),
    do: {view.handle_event(event, state), Reader.advanced(cursor, [entry])}

  defp changed(settings, event, cloud_event) do
    log = event.metadata.agent_id
    fold = &Activity.handle_event(cloud_event, &1)

    case Heads.update(settings.heads, {:activity, log}, Activity.init([]), fold) do
      {same, same} ->
        :ok

      {_before, activity} ->
        Subscribers.publish(settings.group, log, activity_changed(settings, event, activity))
    end
  end

  defp activity_changed(settings, event, activity) do
    CloudEvent.new(
      "activity.changed",
      "agents/" <> event.metadata.agent_id,
      %{"activity" => activity},
      deployment: settings.deployment,
      agent_id: event.metadata.agent_id,
      time: event.time,
      span: event.metadata[:span]
    )
  end

  defp kept(settings, log, cloud_event) do
    case Log.append(settings.store, settings.heads, log, cloud_event) do
      {:ok, sequenced} ->
        Subscribers.publish(settings.group, log, sequenced)

      {:error, reason} ->
        Logger.error("Couldn't keep #{cloud_event["type"]} for #{log}: #{inspect(reason)}")
    end
  end

  defp settings(events), do: Heads.settings(Module.concat(events, Heads))

  defp after_cursor(nil), do: {:ok, %{}}
  defp after_cursor(text), do: Cursor.decode(text)

  defp stream_cursor(nil), do: {:ok, nil}
  defp stream_cursor(text), do: Cursor.decode(text)
end
