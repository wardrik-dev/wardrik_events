defmodule WardrikEvents.Listener do
  @moduledoc """
  Hands agents' events to your module, one at a time.

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
    * `:resources` - the logs to follow, as `WardrikEvents.stream/3` takes them. `:all` unless set.
    * `:types` - the event types to take. Every type unless set.
    * `:after` - the cursor to start after, if you don't define `c:cursor/1`.

  Any other options are kept in `listener.options`.

  ## Delivery

    * Save `listener.cursor` in the same transaction as your work.
    * If `c:handle/2` raises or doesn't return `:ok`, the listener restarts from `c:cursor/1`.
    * So each event arrives at least once. Drop repeats by the event's `id`.
  """

  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Cursor

  @enforce_keys [:events, :cursor, :options]
  defstruct [:events, :cursor, :options]

  @typedoc "The events name, the cursor after the event being handled, and the options."
  @type t() :: %__MODULE__{events: atom(), cursor: String.t() | nil, options: keyword()}

  @doc "Handles `event`. Returns `:ok` once it's done."
  @callback handle(CloudEvent.t(), t()) :: :ok

  @doc "Returns the cursor to start after, or `nil` for the next event written. `:after` unless defined."
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

  @doc "Returns a child spec that runs `module`'s listener."
  @spec child_spec(module(), keyword()) :: Supervisor.child_spec()
  def child_spec(module, opts),
    do: %{id: module, start: {__MODULE__, :start_link, [module, opts]}, restart: :permanent}

  @doc "Starts `module`'s listener with `opts`, linked to the caller."
  @spec start_link(module(), keyword()) :: {:ok, pid()}
  def start_link(module, opts), do: Task.start_link(fn -> listen(module, opts) end)

  defp listen(module, opts) do
    listener = %__MODULE__{events: Keyword.fetch!(opts, :events), cursor: nil, options: opts}

    {:ok, stream} =
      WardrikEvents.stream(listener.events, Keyword.get(opts, :resources, :all),
        after: module.cursor(listener),
        types: Keyword.get(opts, :types, :all)
      )

    Enum.each(stream, fn {event, cursor} ->
      :ok = module.handle(event, %{listener | cursor: Cursor.encode(cursor)})
    end)
  end
end
