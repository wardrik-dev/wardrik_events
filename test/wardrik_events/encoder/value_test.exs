defmodule WardrikEvents.Encoder.ValueTest do
  use ExUnit.Case, async: true

  alias ReqLLM.Message.ContentPart
  alias WardrikEvents.Encoder.Value

  @message JSV.build!(WardrikProtocol.schema("defs") |> get_in(["$defs", "message"]))

  defp encoded(message) do
    encoded = Value.message(message)
    assert {:ok, _} = JSV.validate(encoded, @message)
    encoded
  end

  test "encodes each kind of part as OpenTelemetry tags it" do
    message = %ReqLLM.Message{
      role: :user,
      content: [
        ContentPart.text("Look at these."),
        ContentPart.thinking("Hm."),
        ContentPart.image_url("https://x.test/a.png"),
        ContentPart.video_url("https://x.test/a.mp4"),
        ContentPart.file_id("file_1", "application/pdf"),
        ContentPart.image("<bytes>", "image/png"),
        %ContentPart{type: :provider_block, metadata: %{}}
      ],
      metadata: %{id: "msg_1"}
    }

    assert %{
             "id" => "msg_1",
             "role" => "user",
             "parts" => [
               %{"type" => "text", "content" => "Look at these."},
               %{"type" => "reasoning", "content" => "Hm."},
               %{"type" => "uri", "modality" => "image", "uri" => "https://x.test/a.png"},
               %{"type" => "uri", "modality" => "video"},
               %{"type" => "file", "file_id" => "file_1", "mime_type" => "application/pdf"},
               %{"type" => "blob", "modality" => "image", "mime_type" => "image/png"}
             ]
           } = encoded(message)
  end

  test "encodes a tool's answer under the call it answers, named for its tool" do
    answer = %ReqLLM.Message{
      role: :tool,
      tool_call_id: "c1",
      name: "echo",
      content: [ContentPart.text("hi")]
    }

    assert %{
             "role" => "tool",
             "name" => "echo",
             "parts" => [%{"type" => "tool_call_response", "id" => "c1", "response" => "hi"}]
           } = encoded(answer)
  end

  test "encodes failures by a short type" do
    assert %{"type" => "error", "message" => "Not found"} = Value.error("Not found")
    assert %{"type" => "timeout"} = Value.error(:timeout)
    assert %{"type" => "http", "message" => "{:http, 500}"} = Value.error({:http, 500})
    assert %{"type" => "error", "message" => "{\"x\", 1}"} = Value.error({"x", 1})
    assert %{"type" => "error", "message" => "[1]"} = Value.error([1])

    assert %{"type" => "ArgumentError", "message" => "bad"} =
             Value.error(%ArgumentError{message: "bad"})
  end

  test "encodes any term as JSON" do
    uri = URI.parse("https://x.test")
    term = %{at: ~U[2026-10-22 12:00:00Z], uri: uri, tags: [:a, nil, true], pid: self(), n: 1}

    assert Value.json(term) == %{
             "at" => "2026-10-22T12:00:00Z",
             "uri" => Value.json(Map.from_struct(uri)),
             "tags" => ["a", nil, true],
             "pid" => inspect(self()),
             "n" => 1
           }

    assert Value.json(%{1 => "one"}) == %{"1" => "one"}
    assert Value.principal(nil) == nil
  end
end
