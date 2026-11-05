defmodule WardrikEvents do
  @moduledoc """
  Keeps each agent's events as CloudEvents, in logs you can read back and follow.

      children = [
        {WardrikStore, store},
        {WardrikEvents, name: MyApp.Events, store: store, deployment: "acme-prod"}
      ]

      WardrikCore.start_agent(
        agent_id: "agt_1",
        model: "anthropic:claude-haiku-4-5",
        events: {WardrikEvents, MyApp.Events}
      )

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, ["agt_1"])
      Enum.each(stream, fn {event, cursor} -> save(event, WardrikEvents.Cursor.encode(cursor)) end)

  Each conversation has its own log, `{agent, conversation}`. The main one is `{agent, nil}`.
  Functions that read take a list of:

    * `"agt_1"` - the agent and all its conversations.
    * `{"agt_1", "C1"}` - one conversation.

  `subscribe/2` also takes `:all`, for every agent's events as they happen. To ship every event
  out, to your warehouse or tracing, give the events a `WardrikEvents.Sink`.

  Every function raises `ArgumentError` if `name` isn't started.
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
  alias WardrikEvents.Sink
  alias WardrikEvents.Subscribers
  alias WardrikEvents.Tail

  @typedoc "One conversation's log. `conversation` is `nil` for the agent's main one."
  @type log() :: {agent :: String.t(), conversation :: String.t() | nil}

  @typedoc "An agent ID, for its main conversation, or a `t:log/0`."
  @type agent() :: String.t() | log()

  @typedoc "The logs to read: agent IDs, each with all its conversations, and single logs."
  @type resources() :: [String.t() | log()]

  @default_limit 100
  @max_limit 500

  @doc """
  Starts the events.

      WardrikEvents.start_link(name: MyApp.Events, store: store, deployment: "acme-prod")

  ## Options

    * `:name` - required. What the events are reached by.
    * `:store` - required. The `WardrikStore` to keep the logs in.
    * `:deployment` - the deployment's name in each event's `source`. `"local"` unless set.
    * `:retention` - days events are kept, or `:infinity`. `30` unless set.
    * `:sinks` - `WardrikEvents.Sink`s, as `module` or `{module, opts}`, handed each event once
      it's kept. `[]` unless set.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :store, deployment: "local", retention: 30, sinks: []])
    Supervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl WardrikCore.Events
  def handle_event(event, events) do
    settings = settings(events)

    log = {event.metadata.agent_id, event.metadata[:conversation]}

    for cloud_event <- Encoder.encode(event, settings.deployment) do
      published = publish(settings, log, cloud_event)
      changed(settings, log, published, event.time, event.metadata[:span])
      sunk(settings, published)
    end

    :ok
  end

  @doc """
  Returns whether `resource` covers `log`.

      WardrikEvents.covers?("agt_1", {"agt_1", "C1"})
      #=> true
  """
  @spec covers?(:all | String.t() | log(), log()) :: boolean()
  def covers?(:all, _log), do: true
  def covers?({_agent, _conversation} = log, log), do: true
  def covers?(agent, {agent, _conversation}) when is_binary(agent), do: true
  def covers?(_resource, _log), do: false

  @doc """
  Writes `{type, subject, data}` to an agent's log, like the agent's own events.

      data = %{"trigger" => %{"name" => "daily"}, "status" => "asked"}
      :ok = WardrikEvents.append(MyApp.Events, "agt_1", {"trigger.fired", "triggers/daily", data})

  `type` is a `WardrikProtocol` event type without its `wardrik.` prefix.

  ## Options

    * `:span` - the `WardrikCore.Span` the event happened in.
  """
  @spec append(atom(), agent(), {String.t(), String.t(), map()}, keyword()) :: :ok
  def append(events, agent, {type, subject, data}, opts \\ []) do
    log = logged(agent)
    settings = settings(events)
    time = DateTime.utc_now()
    span = Keyword.get(opts, :span)

    cloud_event =
      CloudEvent.new(type,
        subject: subject,
        data: data,
        deployment: settings.deployment,
        log: log,
        time: time,
        span: span
      )

    published = publish(settings, log, cloud_event)
    changed(settings, log, published, time, span)
    sunk(settings, published)
  end

  @doc """
  Subscribes the calling process to `resources`, or `:all`. It receives `{WardrikEvents, log, event}`.
  """
  @spec subscribe(atom(), resources() | :all) :: :ok
  def subscribe(events, resources), do: Subscribers.subscribe(settings(events).group, resources)

  @doc "Unsubscribes the calling process from `resources`, or `:all`."
  @spec unsubscribe(atom(), resources() | :all) :: :ok
  def unsubscribe(events, resources),
    do: Subscribers.unsubscribe(settings(events).group, resources)

  @doc "Returns the cursor after the last event. Pass it as `:after` to read only newer events."
  @spec head(atom(), resources()) :: String.t()
  def head(events, resources),
    do: settings(events).store |> Reader.head(resources) |> Cursor.encode()

  @doc """
  Returns a page of kept events, oldest first.

      {:ok, %{events: events, next: next}} = WardrikEvents.page(MyApp.Events, ["agt_1"], limit: 100)
      {:ok, %{events: more}} = WardrikEvents.page(MyApp.Events, ["agt_1"], after: next)

    * `next` is the cursor after the page.
    * Starts with a `wardrik.events.dropped` if retention dropped events after the cursor.
    * Returns `{:error, :invalid_cursor}` for a bad `:after`.

  ## Options

    * `:after` - the cursor to read after. The start unless set.
    * `:limit` - the most events to return. `100` unless set, and never more than `500`.
  """
  @spec page(atom(), resources(), keyword()) ::
          {:ok, %{events: [map()], next: String.t()}} | {:error, :invalid_cursor}
  def page(events, resources, opts \\ []) do
    limit = opts |> Keyword.get(:limit, @default_limit) |> min(@max_limit)

    with {:ok, cursor} <- after_cursor(opts[:after]) do
      {read, next} = Reader.page(settings(events), resources, after: cursor, limit: limit)

      {:ok,
       %{events: for({_log, _sequence, event} <- read, do: event), next: Cursor.encode(next)}}
    end
  end

  @doc """
  Streams `{event, cursor}`: kept events after `:after`, then new ones as they're written.

      {:ok, stream} = WardrikEvents.stream(MyApp.Events, [{"agt_1", "C1"}], after: saved)

      Enum.each(stream, fn {event, cursor} ->
        IO.puts(event["type"])
        save(WardrikEvents.Cursor.encode(cursor))
      end)

    * New events include live ones, like `wardrik.message.delta`.
    * No event is skipped or repeated.
    * Returns `{:error, :invalid_cursor}` for a bad `:after`.

  ## Options

    * `:after` - the cursor to read after. The next event written unless set.
    * `:types` - the event types to take, as `["wardrik.run.completed"]`. Every type unless set.
      A `wardrik.events.dropped` always comes through.
  """
  @spec stream(atom(), resources(), keyword()) ::
          {:ok, Enumerable.t({map(), Cursor.t()})} | {:error, :invalid_cursor}
  def stream(events, resources, opts \\ []) do
    opts = Keyword.validate!(opts, [:after, types: :all])

    with {:ok, cursor} <- stream_cursor(opts[:after]),
         do: {:ok, Tail.stream(settings(events), resources, after: cursor, types: opts[:types])}
  end

  @doc """
  Returns what an agent is doing now. See `WardrikEvents.Activity`.

      WardrikEvents.activity(MyApp.Events, "agt_1")
      WardrikEvents.activity(MyApp.Events, {"agt_1", "C1"})
      #=> %{"doing" => [...], "since" => "2026-10-22T09:00:03.000000Z", "pending" => 1}
  """
  @spec activity(atom(), agent()) :: map()
  def activity(events, agent), do: events |> settings() |> loaded(logged(agent))

  @doc """
  Folds a log's kept events into a `WardrikEvents.View`. Returns its state and the cursor after.

      {timeline, cursor} = WardrikEvents.view(MyApp.Events, "agt_1", WardrikEvents.Timeline)

  `opts` go to `c:WardrikEvents.View.init/1`.
  """
  @spec view(atom(), agent(), module(), keyword()) :: {term(), Cursor.t()}
  def view(events, agent, view, opts \\ []) do
    log = logged(agent)

    events
    |> settings()
    |> Reader.all(log)
    |> Enum.reduce({view.init(opts), %{log => 0}}, &folded(view, &1, &2))
  end

  @doc """
  Writes `{type, subject, data}` to the audit, the log of who changed what.

      change = {"agent.forgotten", "agents/agt_1", %{"agent" => %{"id" => "agt_1"}}}
      :ok = WardrikEvents.record(MyApp.Events, change, actor: %WardrikCore.Principal{id: "user_42"})

  ## Options

    * `:actor` - the `WardrikCore.Principal` who made the change.
    * `:assumed_by` - the principals who acted for them, first to last. `[]` unless set.
    * `:span` - the `WardrikCore.Span` the change happened in.
  """
  @spec record(atom(), {String.t(), String.t(), map()}, keyword()) :: :ok | {:error, term()}
  def record(events, change, opts \\ []) do
    opts = Keyword.validate!(opts, [:actor, :span, assumed_by: []])

    settings = settings(events)

    with {:ok, recorded} <- Audit.record(settings, change, opts),
         do: Sink.deliver(settings.sinks, recorded)
  end

  @doc """
  Returns a page of the audit, oldest first. `next` is the last change's sequence number.

      %{events: changes, next: next} = WardrikEvents.audit(MyApp.Events, limit: 100)
      %{events: more} = WardrikEvents.audit(MyApp.Events, after: next)

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

  @doc "Drops events and audit changes written before `cutoff`. Runs hourly."
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
      sinks: Keyword.fetch!(opts, :sinks),
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

  # Sinks come last, once what the event changed is in place for whoever they tell.
  defp sunk(settings, {:kept, event}), do: Sink.deliver(settings.sinks, event)
  defp sunk(_settings, _not_kept), do: :ok

  defp folded(view, {_log, _sequence, event} = entry, {state, cursor}),
    do: {view.handle_event(event, state), Reader.advanced(cursor, [entry])}

  # Only kept events change what an agent is doing, so activity is a fold of the log.
  defp changed(settings, log, {:kept, event}, time, span) do
    case Heads.update(settings.heads, {:activity, log},
           initial: {0, Activity.init([])},
           with: &advanced(settings, log, event, &1)
         ) do
      {{_before, same}, {_after, same}} ->
        :ok

      {_before, {_sequence, activity}} ->
        Subscribers.publish(
          settings.group,
          log,
          activity_changed(settings, log, activity, time, span)
        )
    end
  end

  defp changed(_settings, _log, _not_kept, _time, _span), do: :ok

  # The cache holds the activity with the last sequence it folded. Another node can write to the
  # log too, as when an agent moves, so a gap is read from the log.
  defp advanced(settings, log, event, {sequence, activity} = cached) do
    if Log.sequence(event) == sequence + 1,
      do: {sequence + 1, Activity.handle_event(event, activity)},
      else: caught_up(settings, log, cached)
  end

  defp loaded(settings, log) do
    {_before, {_sequence, activity}} =
      Heads.update(settings.heads, {:activity, log},
        initial: {0, Activity.init([])},
        with: &caught_up(settings, log, &1)
      )

    activity
  end

  defp caught_up(settings, log, {sequence, _activity} = cached) do
    case Reader.entries(settings, log, after: sequence, limit: 1) do
      [] ->
        cached

      [{_log, _sequence, %{"type" => "wardrik.events.dropped"}}] ->
        refolded(settings, log)

      [_newer] ->
        settings |> Reader.all(log, after: sequence) |> Enum.reduce(cached, &taken/2)
    end
  end

  # Retention dropped events this node never folded, so fold what's left, as after a restart.
  defp refolded(settings, log),
    do: settings |> Reader.all(log) |> Enum.reduce({0, Activity.init([])}, &taken/2)

  defp taken({_log, sequence, event}, {_sequence, activity}),
    do: {sequence, Activity.handle_event(event, activity)}

  defp activity_changed(settings, {agent, _conversation} = log, activity, time, span) do
    CloudEvent.new("activity.changed",
      subject: CloudEvent.subject(agent),
      data: %{"activity" => activity},
      deployment: settings.deployment,
      log: log,
      time: time,
      span: span
    )
  end

  defp kept(settings, log, cloud_event) do
    case Log.append(settings.store, log, cloud_event, heads: settings.heads) do
      {:ok, sequenced} ->
        Subscribers.publish(settings.group, log, sequenced)
        {:kept, sequenced}

      {:error, reason} ->
        Logger.error(
          "Couldn't keep #{cloud_event["type"]} for #{inspect(log)}: #{inspect(reason)}"
        )
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

  defp logged(agent) when is_binary(agent), do: {agent, nil}
  defp logged({_agent, _conversation} = log), do: log

  defp stream_cursor(nil), do: {:ok, nil}
  defp stream_cursor(text), do: Cursor.decode(text)
end
