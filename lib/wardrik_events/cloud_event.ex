defmodule WardrikEvents.CloudEvent do
  @moduledoc """
  Builds events as Wardrik sends them: CloudEvents maps with string keys and the payload under
  `"data"`.
  """

  alias WardrikCore.Span

  @typedoc "A CloudEvent, with string keys."
  @type t() :: %{String.t() => term()}

  @doc """
  Builds an event of `type` about `subject`.

      WardrikEvents.CloudEvent.new("run.started", "runs/0af7...", data,
        deployment: "acme-prod",
        agent_id: "acme/triage",
        time: DateTime.utc_now()
      )

  `type` leaves out the `wardrik.` prefix, which is added.

  ## Options

    * `:time` - required. When it happened.
    * `:deployment` - the deployment's name. Required unless `:source` is set.
    * `:agent_id` - the agent the event is about. Required unless `:source` is set.
    * `:source` - the event's `source`, for an event that isn't an agent's.
    * `:span` - the `WardrikCore.Span` it happened in. A new root span unless set.
  """
  @spec new(String.t(), String.t(), map(), keyword()) :: t()
  def new(type, subject, data, opts) do
    span = opts[:span] || Span.root()
    time = Keyword.fetch!(opts, :time)

    %{
      "specversion" => "1.0",
      "id" => uuid_v7(time),
      "source" => Keyword.get_lazy(opts, :source, fn -> agent_source(opts) end),
      "type" => "wardrik." <> type,
      "subject" => subject,
      "time" => DateTime.to_iso8601(time),
      "datacontenttype" => "application/json",
      "dataschema" => WardrikProtocol.dataschema("wardrik." <> type),
      "traceparent" => Span.traceparent(span),
      "data" => data
    }
    |> put_present("parentspanid", span.parent)
  end

  @doc "Puts the event's place in its log as the `sequence` extension, padded to 12 digits."
  @spec sequenced(t(), pos_integer()) :: t()
  def sequenced(event, sequence),
    do: Map.put(event, "sequence", sequence |> Integer.to_string() |> String.pad_leading(12, "0"))

  @doc "Returns the `source` of an agent's events."
  @spec source(String.t(), String.t()) :: String.t()
  def source(deployment, agent_id), do: "wardrik://#{deployment}/agents/#{agent_id}"

  @doc "Returns the `source` of the audit's changes."
  @spec audit_source(String.t()) :: String.t()
  def audit_source(deployment), do: "wardrik://#{deployment}/audit"

  defp agent_source(opts),
    do: source(Keyword.fetch!(opts, :deployment), Keyword.fetch!(opts, :agent_id))

  @doc "Returns the trace id in the event's `traceparent`, which is its run's id. Returns `nil` if there's none."
  @spec trace_id(t()) :: String.t() | nil
  def trace_id(%{"traceparent" => "00-" <> <<trace_id::binary-32, "-", _rest::binary>>}),
    do: trace_id

  def trace_id(_event), do: nil

  defp uuid_v7(time) do
    milliseconds = DateTime.to_unix(time, :millisecond)
    <<a::12, b::62, _rest::6>> = :crypto.strong_rand_bytes(10)

    <<milliseconds::48, 7::4, a::12, 2::2, b::62>>
    |> Base.encode16(case: :lower)
    |> hyphenated()
  end

  defp hyphenated(<<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>>),
    do: Enum.join([a, b, c, d, e], "-")

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)
end
