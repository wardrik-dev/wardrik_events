defmodule WardrikEvents do
  @moduledoc """
  Keeps each agent's events as CloudEvents, in a log you can read back and follow.

      children = [
        {WardrikStore, store},
        {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
      ]

      WardrikCore.start_agent(
        agent_id: "acme/triage",
        model: "anthropic:claude-haiku-4-5",
        events: {WardrikEvents, MyApp.Events}
      )

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/*"])
      Enum.each(stream, fn {event, cursor} -> save(event, WardrikEvents.Cursor.encode(cursor)) end)

  Each agent writes one log, named by its id. Functions take logs as patterns, where `*` matches
  any run of characters and `?` matches one. See `WardrikEvents.Pattern`.

  Functions that take the events' name raise `ArgumentError` if they aren't started.
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
  Starts the events.

      WardrikEvents.start_link(name: MyApp.Events, store: store, deployment: "acme-prod")

  ## Options

    * `:name` - required. What the events are reached by.
    * `:store` - required. The `WardrikStore` to keep the logs in.
    * `:deployment` - the deployment's name in each event's `source`. `"local"` unless set.
    * `:retention` - days events are kept, or `:infinity`. `30` unless set. Older events are
      dropped hourly.
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
      changed(settings, event.metadata.agent_id, cloud_event, event.time, event.metadata[:span])
    end

    :ok
  end

  @doc """
  Writes an event into the log of the agent `agent_id`.

      data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}
      :ok = WardrikEvents.append(MyApp.Events, "acme/triage", {"trigger.fired", "triggers/daily", data})

  The event is `{type, subject, data}`, with a type from `WardrikProtocol`. It's handled like the
  agent's own events: kept, sent to subscribers and folded into its activity.

  ## Options

    * `:span` - the `WardrikCore.Span` the event happened in.
  """
  @spec append(atom(), String.t(), {String.t(), String.t(), map()}, keyword()) :: :ok
  def append(events, agent_id, {type, subject, data}, opts \\ []) do
    settings = settings(events)
    time = DateTime.utc_now()
    span = Keyword.get(opts, :span)

    cloud_event =
      CloudEvent.new(type, subject, data,
        deployment: settings.deployment,
        agent_id: agent_id,
        time: time,
        span: span
      )

    publish(settings, agent_id, cloud_event)
    changed(settings, agent_id, cloud_event, time, span)
    :ok
  end

  @doc """
  Subscribes the calling process to the logs `resources` match.

  The process is sent `{WardrikEvents, log, event}` for each event, live or kept.
  """
  @spec subscribe(atom(), [String.t()]) :: :ok
  def subscribe(events, resources), do: Subscribers.subscribe(settings(events).group, resources)

  @doc "Unsubscribes the calling process from `resources`."
  @spec unsubscribe(atom(), [String.t()]) :: :ok
  def unsubscribe(events, resources),
    do: Subscribers.unsubscribe(settings(events).group, resources)

  @doc "Returns the cursor at the end of the logs `resources` match. Pass it as `:after` to read only newer events."
  @spec head(atom(), [String.t()]) :: String.t()
  def head(events, resources),
    do: settings(events).store |> Reader.head(resources) |> Cursor.encode()

  @doc """
  Returns a page of kept events from the logs `resources` match, oldest first.

      {:ok, %{events: events, next: next}} = WardrikEvents.page(MyApp.Events, ["acme/*"], limit: 100)
      {:ok, %{events: more}} = WardrikEvents.page(MyApp.Events, ["acme/*"], after: next)

  `next` is the cursor after the page. Returns `{:error, :invalid_cursor}` if `:after` can't be
  decoded.

  If retention dropped events past the cursor, the page starts with a `wardrik.events.dropped`.

  ## Options

    * `:after` - the cursor to read after. The start of the logs unless set.
    * `:limit` - the most events to return. `100` unless set, and never more than `500`.
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
  Streams the events of the logs `resources` match, as `{event, cursor}`.

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, ["acme/*"], after: saved)

      Enum.each(stream, fn {event, cursor} ->
        show(event)
        save(WardrikEvents.Cursor.encode(cursor))
      end)

  The stream reads back kept events, then follows new ones, live events too. It never skips an
  event or repeats one between the two. It runs until the caller stops taking from it.

  Returns `{:error, :invalid_cursor}` if `:after` can't be decoded.

  ## Options

    * `:after` - the cursor to read after, as `WardrikEvents.Cursor.encode/1` returns it. Only
      events written from now on unless set.
    * `:types` - the event types to take, as `["wardrik.run.completed"]`. Every type unless set.
      A `wardrik.events.dropped` always comes through.
  """
  @spec stream(atom(), [String.t()], keyword()) ::
          {:ok, Enumerable.t({map(), Cursor.t()})} | {:error, :invalid_cursor}
  def stream(events, resources, opts \\ []) do
    opts = Keyword.validate!(opts, [:after, types: :all])

    with {:ok, cursor} <- stream_cursor(opts[:after]),
         do: {:ok, Tail.stream(settings(events), resources, cursor, opts[:types])}
  end

  @doc """
  Returns what the agent writing `log` is doing now, as the protocol's `activity`.

      WardrikEvents.activity(MyApp.Events, "acme/triage")
      #=> %{"doing" => [...], "since" => "2026-10-22T09:00:03.000000Z", "pending" => 1}

  See `WardrikEvents.Activity`.
  """
  @spec activity(atom(), String.t()) :: map()
  def activity(events, log) do
    case :ets.lookup(settings(events).heads, {:activity, log}) do
      [{_key, _version, activity}] -> activity
      [] -> Activity.init([])
    end
  end

  @doc """
  Folds every event `log` keeps into `view`. Returns the view's state and the cursor after the last
  event.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "acme/triage", WardrikEvents.Timeline)

  `view` is a `WardrikEvents.View`. `opts` are passed to its `c:WardrikEvents.View.init/1`.
  """
  @spec view(atom(), String.t(), module(), keyword()) :: {term(), Cursor.t()}
  def view(events, log, view, opts \\ []) do
    events
    |> settings()
    |> Reader.all(log)
    |> Enum.reduce({view.init(opts), %{log => 0}}, &folded(view, &1, &2))
  end

  @doc """
  Records a change someone made in the audit.

      change = {"agent.forgotten", "agents/acme/triage", %{"agent" => %{"id" => "acme/triage"}}}
      :ok = WardrikEvents.record(MyApp.Events, change, actor: principal)

  `change` is `{type, subject, data}`. The audit is one log for the deployment, kept apart from the
  agents' logs.

  ## Options

    * `:actor` - the `WardrikCore.Principal` who made the change.
    * `:assumed_by` - the principals who acted for the actor, first to last. `[]` unless set.
    * `:span` - the `WardrikCore.Span` the change happened in.
  """
  @spec record(atom(), {String.t(), String.t(), map()}, keyword()) :: :ok | {:error, term()}
  def record(events, change, opts \\ []) do
    opts = Keyword.validate!(opts, [:actor, :span, assumed_by: []])

    with {:ok, _event} <- Audit.record(settings(events), change, opts), do: :ok
  end

  @doc """
  Returns a page of the audit, oldest first.

      %{events: changes, next: next} = WardrikEvents.audit(MyApp.Events, limit: 100)
      %{events: more} = WardrikEvents.audit(MyApp.Events, after: next)

  `next` is the sequence number of the last change returned.

  ## Options

    * `:after` - the sequence number to read after. `0` unless set.
    * `:limit` - the most changes to return. `100` unless set, and never more than `500`.
  """
  @spec audit(atom(), keyword()) :: %{events: [map()], next: non_neg_integer()}
  def audit(events, opts \\ []) do
    limit = opts |> Keyword.get(:limit, @default_limit) |> min(@max_limit)
    {changes, next} = Audit.page(settings(events), Keyword.get(opts, :after, 0), limit)
    %{events: changes, next: next}
  end

  @doc "Drops the events and audit changes written before `cutoff`. Retention calls this hourly."
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
      heads: Module.concat(name, "Heads"),
      group: Module.concat(name, "Group")
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

  defp changed(settings, log, cloud_event, time, span) do
    fold = &Activity.handle_event(cloud_event, &1)

    case Heads.update(settings.heads, {:activity, log}, Activity.init([]), fold) do
      {same, same} ->
        :ok

      {_before, activity} ->
        Subscribers.publish(
          settings.group,
          log,
          activity_changed(settings, log, activity, time, span)
        )
    end
  end

  defp activity_changed(settings, log, activity, time, span) do
    CloudEvent.new(
      "activity.changed",
      "agents/" <> log,
      %{"activity" => activity},
      deployment: settings.deployment,
      agent_id: log,
      time: time,
      span: span
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

  defp settings(events) do
    Heads.settings(Module.concat(events, "Heads"))
  rescue
    ArgumentError ->
      reraise ArgumentError,
              "#{inspect(events)} isn't started. Add {WardrikEvents, name: #{inspect(events)}, store: store} to your supervision tree, ahead of what uses it.",
              __STACKTRACE__
  end

  defp after_cursor(nil), do: {:ok, %{}}
  defp after_cursor(text), do: Cursor.decode(text)

  defp stream_cursor(nil), do: {:ok, nil}
  defp stream_cursor(text), do: Cursor.decode(text)
end
