defmodule WardrikEvents.Listener do
  @moduledoc """
  A listener follows agents' events into your app. It hands each event to your module with the
  cursor past it, so you keep your place with your own work and carry on after any restart.

      defmodule MyApp.Runs do
        use WardrikEvents.Listener

        @impl true
        def handle(event, listener) do
          MyApp.Repo.transaction(fn ->
            MyApp.Repo.insert_all("agent_events", [MyApp.Runs.row(event)], on_conflict: :nothing)
            MyApp.Cursors.put!("runs", WardrikEvents.Cursor.encode(listener.cursor))
          end)

          :ok
        end

        @impl true
        def cursor(_listener), do: MyApp.Cursors.get("runs")
      end

      children = [{MyApp.Runs, events: MyApp.Events, types: ["wardrik.run.completed"]}]

  It takes `:events`, `:resources` (every agent unless set), `:types` (every type unless set) and
  `:after`, the cursor `cursor/1` returns unless you define it, and your module's own options
  beside them. Without a cursor it reads from what's written next.

  Each event is handed over at least once: save the cursor with what `handle/2` did, in one
  transaction, and drop a repeat by the event's `id`. A `handle/2` that raises or returns anything
  but `:ok` restarts the listener, which asks `cursor/1` again. A `wardrik.events.dropped` comes
  whatever `:types` says.
  """

  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Cursor

  @enforce_keys [:events, :cursor, :options]
  defstruct [:events, :cursor, :options]

  @typedoc "The events it reads, the cursor past the event being handled, and its options."
  @type t() :: %__MODULE__{events: atom(), cursor: Cursor.t() | nil, options: keyword()}

  @doc "Does what your app does with `event`, and returns `:ok` once it's done."
  @callback handle(CloudEvent.t(), t()) :: :ok

  @doc "Returns the cursor to read after as the listener starts, or `nil` for what's written next."
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

  @doc "Returns the child that runs `module`'s listener, a permanent `Task`."
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
      :ok = module.handle(event, %{listener | cursor: cursor})
    end)
  end
end
