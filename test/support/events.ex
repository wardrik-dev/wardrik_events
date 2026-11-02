defmodule WardrikEvents.Test.Events do
  @moduledoc "Starts events over an ETS store for a test, and writes agents' events to them."

  import ExUnit.Callbacks, only: [start_supervised!: 1]

  alias WardrikCore.Event

  def start_events(context) do
    store = WardrikStore.ETS.new(name: :"store #{context.test}")
    events = :"events #{context.test}"
    start_supervised!({WardrikStore, store})
    start_supervised!({WardrikEvents, name: events, store: store, deployment: "test"})
    trimmed(events)
    %{events: events, store: store}
  end

  def trimmed(events) do
    {WardrikEvents.Retention, retention, :worker, _modules} =
      events |> Supervisor.which_children() |> List.keyfind(WardrikEvents.Retention, 0)

    :sys.get_state(retention)
  end

  @doc "Emits a core event into `log`: an agent ID for the agent's own, or `{agent, conversation}`."
  def emit(events, log, name, metadata, time \\ DateTime.utc_now()) do
    {agent, conversation} = log(log)

    event = %Event{
      name: [:wardrik_core | name],
      time: time,
      metadata: Map.merge(metadata, %{agent_id: agent, conversation: conversation})
    }

    :ok = WardrikEvents.handle_event(event, events)
  end

  def stopped(events, log, time \\ DateTime.utc_now()),
    do: emit(events, log, [:agent, :stop], %{reason: :normal}, time)

  defp log({_agent, _conversation} = log), do: log
  defp log(agent), do: {agent, nil}

  def sequences(events, resources, opts \\ []) do
    {:ok, %{events: read}} = WardrikEvents.page(events, resources, opts)
    Enum.map(read, &(&1["sequence"] || &1["type"]))
  end
end

defmodule WardrikEvents.Test.Protocol do
  @moduledoc "Validates CloudEvents against the protocol's schemas."

  import ExUnit.Assertions

  def valid!(cloud_event) do
    envelope = JSV.build!(WardrikProtocol.schema("cloudevent"))
    assert {:ok, _event} = JSV.validate(cloud_event, envelope)
    "wardrik." <> type = cloud_event["type"]

    assert {:ok, _data} =
             JSV.validate(cloud_event["data"], JSV.build!(WardrikProtocol.schema(type)))

    cloud_event
  end
end
