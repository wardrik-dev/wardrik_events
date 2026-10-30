defmodule WardrikEvents.Listener do
  @moduledoc """
  Follows agents' events into your app.

  A listener hands each event to `c:handle/2`, with the cursor past it. Save the cursor with your
  work, and return it from `c:cursor/1`. The listener then carries on from there after a restart.

      defmodule MyApp.Runs do
        use WardrikEvents.Listener

        @impl true
        def handle(event, listener) do
          MyApp.Repo.transaction(fn ->
            MyApp.Repo.insert_all("agent_events", [MyApp.Runs.row(event)], on_conflict: :nothing)
            MyApp.Cursors.put!("runs", listener.cursor)
          end)

          :ok
        end

        @impl true
        def cursor(_listener), do: MyApp.Cursors.get("runs")
      end

      children = [{MyApp.Runs, events: MyApp.Events, types: ["wardrik.run.completed"]}]

  ## Options

    * `:events` - required. The name of your `WardrikEvents`.
    * `:resources` - patterns of the logs to follow. `["*"]`, every agent, unless set.
    * `:types` - the event types to take. Every type unless set. A `wardrik.events.dropped`
      always comes through.
    * `:after` - the cursor to start after, if you don't define `c:cursor/1`. What's written next
      unless set.

  Your module's own options can sit beside these. They're in the listener's `:options`.

  ## Delivery

  Each event is handed over at least once.

    1. Save the cursor in the same transaction as your work.
    2. If `c:handle/2` raises or returns anything but `:ok`, the listener restarts.
    3. On restart it calls `c:cursor/1` again, and reads after what it returns.

  So drop a repeat by the event's `id`.
  """

  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Cursor

  @enforce_keys [:events, :cursor, :options]
  defstruct [:events, :cursor, :options]

  @typedoc "The listener's events name, the cursor past the event being handled, and its options."
  @type t() :: %__MODULE__{events: atom(), cursor: String.t() | nil, options: keyword()}

  @doc "Handles `event`. Returns `:ok` once it's done."
  @callback handle(CloudEvent.t(), t()) :: :ok

  @doc "Returns the cursor to read after as the listener starts, or `nil` for what's written next. Returns `:after` unless defined."
  @callback cursor(t()) :: String.t() | nil

  defmacro __using__(_opts) do
    quote do
      @behaviour WardrikEvents.Listener

      @doc false
      def child_spec(opts), do: WardrikEvents.Listener.child_spec(__MODULE__, opts)

      @doc false
      def start_link(opts), do: WardrikEvents.Listener.start_link(__MODULE__, opts)

      @impl WardrikEvents.Listener
      def cursor(listener), do: listener.options[:after]

      defoverridable child_spec: 1, cursor: 1
    end
  end

  @doc "Returns the child spec that runs `module`'s listener as a permanent `Task`."
  @spec child_spec(module(), keyword()) :: Supervisor.child_spec()
  def child_spec(module, opts),
    do: %{id: module, start: {__MODULE__, :start_link, [module, opts]}, restart: :permanent}

  @doc "Starts `module`'s listener with `opts`, linked to the caller."
  @spec start_link(module(), keyword()) :: {:ok, pid()}
  def start_link(module, opts), do: Task.start_link(fn -> listen(module, opts) end)

  defp listen(module, opts) do
    listener = %__MODULE__{events: Keyword.fetch!(opts, :events), cursor: nil, options: opts}

    {:ok, stream} =
      WardrikEvents.stream(listener.events, Keyword.get(opts, :resources, ["*"]),
        after: module.cursor(listener),
        types: Keyword.get(opts, :types, :all)
      )

    Enum.each(stream, fn {event, cursor} ->
      :ok = module.handle(event, %{listener | cursor: Cursor.encode(cursor)})
    end)
  end
end
