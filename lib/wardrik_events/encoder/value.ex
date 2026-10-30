defmodule WardrikEvents.Encoder.Value do
  @moduledoc """
  Encodes the values inside an event's data as JSON-ready maps with string keys.
  """

  alias ReqLLM.Message.ContentPart
  alias WardrikCore.Principal

  @limit 64 * 1024

  @doc "Encodes a message. Uses `id`, or the id in its metadata."
  @spec message(ReqLLM.Message.t(), String.t() | nil) :: map()
  def message(%ReqLLM.Message{} = message, id \\ nil) do
    %{"role" => Atom.to_string(message.role), "parts" => parts(message)}
    |> put("id", id || message.metadata[:id])
    |> put("name", message.name)
  end

  @doc "Encodes why something failed, as OpenTelemetry's `error.type` and a message."
  @spec error(term()) :: map()
  def error(%{__exception__: true} = exception),
    do: %{"type" => short_name(exception.__struct__), "message" => Exception.message(exception)}

  def error(reason) when is_binary(reason), do: %{"type" => "error", "message" => reason}

  def error(reason) when is_atom(reason),
    do: %{"type" => Atom.to_string(reason), "message" => inspect(reason)}

  def error(reason) when is_tuple(reason) and tuple_size(reason) > 0,
    do: tagged(elem(reason, 0), reason)

  def error(reason), do: tagged(nil, reason)

  @doc "Encodes the tokens a model call read and wrote. Returns `nil` if there are none."
  @spec usage(map()) :: map() | nil
  def usage(%{input_tokens: input, output_tokens: output}),
    do: %{"input_tokens" => input, "output_tokens" => output}

  def usage(_measurements), do: nil

  @doc "Encodes a call's cost as whole micro-dollars. Returns `nil` if it isn't priced."
  @spec cost(map(), String.t()) :: map() | nil
  def cost(%{usd: usd}, source), do: %{"micro_usd" => round(usd * 1_000_000), "source" => source}
  def cost(_measurements, _source), do: nil

  @doc "Encodes who did something."
  @spec principal(Principal.t() | nil) :: map() | nil
  def principal(nil), do: nil

  def principal(%Principal{id: id, attributes: attributes}),
    do: put(%{"id" => id}, "name", attributes[:name])

  @doc "Encodes any term as JSON. Maps get string keys, atoms become strings and structs become maps."
  @spec json(term()) :: term()
  def json(%Principal{} = principal), do: principal(principal)
  def json(%DateTime{} = time), do: DateTime.to_iso8601(time)
  def json(%{__struct__: _module} = struct), do: struct |> Map.from_struct() |> json()
  def json(%{} = map), do: Map.new(map, fn {key, value} -> {json_key(key), json(value)} end)
  def json(list) when is_list(list), do: Enum.map(list, &json/1)
  def json(value) when is_boolean(value) or is_nil(value), do: value
  def json(value) when is_atom(value), do: Atom.to_string(value)
  def json(value) when is_binary(value) or is_number(value), do: value
  def json(value), do: inspect(value)

  @doc "Puts `value` under `key`. If it's longer than 64 KiB, cuts it short and sets `\"truncated\"`."
  @spec capped(map(), String.t(), term()) :: map()
  def capped(map, key, value) do
    encoded = value |> json() |> JSON.encode!()

    if byte_size(encoded) > @limit,
      do: map |> Map.put(key, binary_part(encoded, 0, @limit)) |> Map.put("truncated", true),
      else: Map.put(map, key, json(value))
  end

  @doc "Puts `value` under `key`, unless it's `nil`."
  @spec put(map(), String.t(), term()) :: map()
  def put(map, _key, nil), do: map
  def put(map, key, value), do: Map.put(map, key, value)

  defp parts(%ReqLLM.Message{role: :tool, tool_call_id: id} = message),
    do: [capped(%{"type" => "tool_call_response", "id" => id}, "response", text(message))]

  defp parts(%ReqLLM.Message{content: content, tool_calls: calls}),
    do: Enum.flat_map(content, &part/1) ++ Enum.map(calls || [], &tool_call/1)

  defp part(%ContentPart{type: :text, text: text}), do: [text_part("text", text)]
  defp part(%ContentPart{type: :thinking, text: text}), do: [text_part("reasoning", text)]
  defp part(%ContentPart{type: :image_url, url: url}), do: [uri(url, "image")]
  defp part(%ContentPart{type: :video_url, url: url}), do: [uri(url, "video")]

  defp part(%ContentPart{type: :file, file_id: id} = part) when is_binary(id),
    do: [
      put(
        %{"type" => "file", "modality" => "document", "file_id" => id},
        "mime_type",
        part.media_type
      )
    ]

  defp part(%ContentPart{type: type} = part) when type in [:image, :file],
    do: [put(%{"type" => "blob", "modality" => modality(type)}, "mime_type", part.media_type)]

  defp part(_provider_block), do: []

  defp tool_call(call) do
    capped(
      %{"type" => "tool_call", "id" => call.id, "name" => ReqLLM.ToolCall.name(call)},
      "arguments",
      ReqLLM.ToolCall.args_map(call)
    )
  end

  defp text_part(type, text), do: capped(%{"type" => type}, "content", text)

  defp uri(url, modality), do: %{"type" => "uri", "modality" => modality, "uri" => url}

  defp modality(:image), do: "image"
  defp modality(:file), do: "document"

  defp text(%ReqLLM.Message{content: content}),
    do: Enum.map_join(content, fn part -> part.text || "" end)

  defp tagged(tag, reason) when is_atom(tag) and not is_nil(tag),
    do: %{"type" => Atom.to_string(tag), "message" => inspect(reason)}

  defp tagged(_tag, reason), do: %{"type" => "error", "message" => inspect(reason)}

  defp json_key(key) when is_atom(key), do: Atom.to_string(key)
  defp json_key(key), do: to_string(key)

  defp short_name(module), do: module |> Module.split() |> List.last()
end
