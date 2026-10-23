defmodule WardrikEvents.Cursor do
  @moduledoc "A cursor is where a reader has got to in each log, as the sequence number it read through."

  @typedoc "Each log's sequence number."
  @type t() :: %{String.t() => non_neg_integer()}

  @doc "Encodes a cursor as text a client keeps."
  @spec encode(t()) :: String.t()
  def encode(cursor), do: cursor |> JSON.encode!() |> Base.url_encode64(padding: false)

  @doc "Decodes a cursor a client kept."
  @spec decode(String.t()) :: {:ok, t()} | {:error, :invalid_cursor}
  def decode(text) do
    with {:ok, json} <- Base.url_decode64(text, padding: false),
         {:ok, %{} = cursor} <- JSON.decode(json),
         true <- Enum.all?(cursor, &sequence?/1) do
      {:ok, cursor}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp sequence?({_log, sequence}), do: is_integer(sequence) and sequence >= 0
end
