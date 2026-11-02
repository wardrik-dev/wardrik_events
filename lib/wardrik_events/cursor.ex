defmodule WardrikEvents.Cursor do
  @moduledoc """
  Encodes and decodes cursors. A cursor maps each log to the last sequence number read.

      WardrikEvents.Cursor.encode(%{{"agt_1", nil} => 41, {"agt_1", "C1"} => 3})
  """

  @typedoc "The last sequence number read in each log."
  @type t() :: %{WardrikEvents.log() => non_neg_integer()}

  @doc "Encodes a cursor as URL-safe text, for `:after`."
  @spec encode(t()) :: String.t()
  def encode(cursor) do
    cursor
    |> Enum.map(fn {{agent, conversation}, sequence} -> [agent, conversation, sequence] end)
    |> Enum.sort()
    |> JSON.encode!()
    |> Base.url_encode64(padding: false)
  end

  @doc "Decodes text from `encode/1`, or returns `{:error, :invalid_cursor}`."
  @spec decode(String.t()) :: {:ok, t()} | {:error, :invalid_cursor}
  def decode(text) do
    with {:ok, json} <- Base.url_decode64(text, padding: false),
         {:ok, entries} when is_list(entries) <- JSON.decode(json),
         true <- Enum.all?(entries, &entry?/1) do
      {:ok,
       Map.new(entries, fn [agent, conversation, sequence] ->
         {{agent, conversation}, sequence}
       end)}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp entry?([agent, conversation, sequence]),
    do:
      is_binary(agent) and (is_binary(conversation) or is_nil(conversation)) and
        is_integer(sequence) and sequence >= 0

  defp entry?(_entry), do: false
end
