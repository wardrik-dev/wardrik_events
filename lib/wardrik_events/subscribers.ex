defmodule WardrikEvents.Subscribers do
  @moduledoc "Subscribers are the processes sent each event as it's written, to the logs their patterns cover."

  alias WardrikEvents.Pattern

  @doc "Subscribes the calling process to the logs `resources` cover."
  @spec subscribe(atom(), [String.t()]) :: :ok
  def subscribe(group, resources) do
    for {topic, patterns} <- by_topic(resources) do
      :ok = Group.join(group, topic, %{patterns: Enum.uniq(held(group, topic) ++ patterns)})
    end

    :ok
  end

  @doc "Unsubscribes the calling process from the logs `resources` cover."
  @spec unsubscribe(atom(), [String.t()]) :: :ok
  def unsubscribe(group, resources) do
    for {topic, patterns} <- by_topic(resources) do
      case held(group, topic) -- patterns do
        [] -> Group.leave(group, topic)
        held -> Group.join(group, topic, %{patterns: held})
      end
    end

    :ok
  end

  @doc "Sends `event` to every process subscribed to `log`, as `{WardrikEvents, log, event}`."
  @spec publish(atom(), String.t(), map()) :: :ok
  def publish(group, log, event) do
    takers =
      for topic <- Enum.uniq([topic(log), topic("*")]),
          {pid, %{patterns: patterns}} <- Group.members(group, topic),
          Enum.any?(patterns, &Pattern.covers?(&1, log)),
          uniq: true,
          do: pid

    Enum.each(takers, &send(&1, {WardrikEvents, log, event}))
  end

  defp by_topic(resources), do: Enum.group_by(resources, &topic/1)

  defp held(group, topic) do
    Enum.concat(
      for {pid, %{patterns: patterns}} <- Group.local_members(group, topic),
          pid == self(),
          do: patterns
    )
  end

  defp topic(pattern), do: "org:" <> Pattern.org(pattern)
end
