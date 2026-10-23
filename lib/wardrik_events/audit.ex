defmodule WardrikEvents.Audit do
  @moduledoc """
  The audit is one log for the deployment of what people changed and who changed it, beside each
  agent's own.
  """

  alias WardrikEvents.CloudEvent
  alias WardrikEvents.Encoder.Value
  alias WardrikEvents.Log
  alias WardrikEvents.Reader

  @log "audit"

  @doc "Writes `change`, a `{type, subject, data}`, with who made it and who acted for them."
  @spec record(map(), {String.t(), String.t(), map()}, keyword()) ::
          {:ok, CloudEvent.t()} | {:error, term()}
  def record(settings, {type, subject, data}, opts) do
    data =
      data
      |> Value.put("actor", Value.principal(opts[:actor]))
      |> Value.put("assumed_by", assumed_by(opts[:assumed_by]))

    event =
      CloudEvent.new(type, subject, data,
        source: CloudEvent.audit_source(settings.deployment),
        deployment: settings.deployment,
        time: DateTime.utc_now(),
        span: opts[:span]
      )

    Log.append(settings.audit, settings.heads, @log, event)
  end

  @doc "Reads up to `limit` changes after the sequence number `at`, and the sequence after them."
  @spec page(map(), non_neg_integer(), pos_integer()) :: {[CloudEvent.t()], non_neg_integer()}
  def page(settings, at, limit) do
    settings
    |> reading()
    |> Reader.entries(@log, at, limit)
    |> Enum.map_reduce(at, fn {_log, sequence, event}, _at -> {event, sequence} end)
  end

  defp reading(settings) do
    %{
      store: settings.audit,
      deployment: settings.deployment,
      source: CloudEvent.audit_source(settings.deployment),
      subject: @log
    }
  end

  defp assumed_by([_principal | _principals] = principals),
    do: Enum.map(principals, &Value.principal/1)

  defp assumed_by(_nobody), do: nil
end
