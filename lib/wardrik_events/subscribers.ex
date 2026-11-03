defmodule WardrikEvents.Subscribers do
  @moduledoc false

  @all "all"

  @doc "Subscribes the calling process to `resources`. See `WardrikEvents.resources()`."
  @spec subscribe(atom(), WardrikEvents.resources() | :all) :: :ok
  def subscribe(group, resources) do
    for {topic, covered} <- by_topic(resources) do
      :ok = Group.join(group, topic, %{covers: Enum.uniq(held(group, topic) ++ covered)})
    end

    :ok
  end

  @doc "Unsubscribes the calling process from `resources`."
  @spec unsubscribe(atom(), WardrikEvents.resources() | :all) :: :ok
  def unsubscribe(group, resources) do
    for {topic, covered} <- by_topic(resources) do
      case held(group, topic) -- covered do
        [] -> Group.leave(group, topic)
        held -> Group.join(group, topic, %{covers: held})
      end
    end

    :ok
  end

  @doc "Sends `event` to every process subscribed to `log`, as `{WardrikEvents, log, event}`."
  @spec publish(atom(), WardrikEvents.log(), map()) :: :ok
  def publish(group, {agent, _conversation} = log, event) do
    takers =
      for topic <- [topic(agent), @all],
          {pid, %{covers: covers}} <- Group.members(group, topic),
          Enum.any?(covers, &WardrikEvents.covers?(&1, log)),
          uniq: true,
          do: pid

    Enum.each(takers, &send(&1, {WardrikEvents, log, event}))
  end

  defp by_topic(:all), do: %{@all => [:all]}

  defp by_topic(resources),
    do: Enum.group_by(resources, &resource_topic/1)

  defp resource_topic({agent, _conversation}), do: topic(agent)
  defp resource_topic(agent), do: topic(agent)

  defp held(group, topic) do
    Enum.concat(
      for {pid, %{covers: covers}} <- Group.local_members(group, topic),
          pid == self(),
          do: covers
    )
  end

  defp topic(agent), do: "agent:" <> URI.encode_www_form(agent)
end
