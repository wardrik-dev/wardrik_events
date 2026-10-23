defmodule WardrikEvents.AuditTest do
  use ExUnit.Case, async: false
  use Mimic

  import WardrikEvents.Test.Events
  import WardrikEvents.Test.Protocol

  alias WardrikCore.Principal

  setup :start_events

  defp forgotten(agent),
    do: {"agent.forgotten", "agents/" <> agent, %{"agent" => %{"id" => agent}}}

  test "records who changed what, and who acted for them", %{events: events} do
    actor = %Principal{id: "acme/@jon", attributes: %{name: "Jon"}}
    assistant = %Principal{id: "acme/agent/helper"}

    assert :ok =
             WardrikEvents.record(events, forgotten("acme/triage"),
               actor: actor,
               assumed_by: [assistant]
             )

    assert :ok = WardrikEvents.record(events, forgotten("acme/digest"))

    assert %{events: [first, second], next: 2} = WardrikEvents.audit(events)

    assert %{
             "source" => "wardrik://test/audit",
             "subject" => "agents/acme/triage",
             "sequence" => "000000000001",
             "data" => %{
               "agent" => %{"id" => "acme/triage"},
               "actor" => %{"id" => "acme/@jon", "name" => "Jon"},
               "assumed_by" => [%{"id" => "acme/agent/helper"}]
             }
           } = valid!(first)

    refute Map.has_key?(valid!(second)["data"], "actor")
  end

  test "pages through the changes", %{events: events} do
    for agent <- ["a/1", "a/2", "a/3"], do: :ok = WardrikEvents.record(events, forgotten(agent))

    assert %{events: [_, _], next: 2} = WardrikEvents.audit(events, limit: 2)

    assert %{events: [%{"sequence" => "000000000003"}], next: 3} =
             WardrikEvents.audit(events, after: 2)

    assert %{events: [], next: 3} = WardrikEvents.audit(events, after: 3)
  end

  test "keeps its numbers apart from an agent named audit", %{events: events} do
    stopped(events, "audit")
    :ok = WardrikEvents.record(events, forgotten("audit"))

    assert sequences(events, ["audit"]) == ["000000000001"]
    assert %{events: [%{"sequence" => "000000000001"}]} = WardrikEvents.audit(events)
  end

  test "says so when the changes asked for were trimmed", %{events: events} do
    :ok = WardrikEvents.record(events, forgotten("a/1"))
    :ok = WardrikEvents.trim(events, DateTime.add(DateTime.utc_now(), 1, :day))

    assert %{events: [dropped], next: 1} = WardrikEvents.audit(events)

    assert %{"type" => "wardrik.events.dropped", "source" => "wardrik://test/audit"} =
             valid!(dropped)
  end

  test "returns the store's refusal", %{events: events} do
    expect(WardrikStore, :put, fn _store, _key, _value, _opts -> {:error, :down} end)

    assert {:error, :down} = WardrikEvents.record(events, forgotten("a/1"))
  end
end
