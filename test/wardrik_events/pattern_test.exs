defmodule WardrikEvents.PatternTest do
  use ExUnit.Case, async: true

  alias WardrikEvents.Pattern

  test "covers the logs it names" do
    assert Pattern.covers?("acme/triage", "acme/triage")
    assert Pattern.covers?("acme/*", "acme/triage")
    assert Pattern.covers?("*", "acme/teams/triage")
    assert Pattern.covers?("*/triage", "acme/triage")
    assert Pattern.covers?("acme/tri?ge", "acme/triäge")
    assert Pattern.covers?("acme/*ge", "acme/triage")

    refute Pattern.covers?("acme/triage", "acme/triage2")
    refute Pattern.covers?("acme/?", "acme/")
    refute Pattern.covers?("acme/*", "other/triage")
    refute Pattern.covers?("acme/*x", "acme/triage")
  end

  test "names the org, or every org when it starts with a wildcard" do
    assert Pattern.org("acme/*") == "acme"
    assert Pattern.org("acme") == "acme"
    assert Pattern.org("ac*/triage") == "*"
    assert Pattern.org("?cme/triage") == "*"
  end
end
