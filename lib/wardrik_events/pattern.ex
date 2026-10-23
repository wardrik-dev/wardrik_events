defmodule WardrikEvents.Pattern do
  @moduledoc "A pattern names logs: `*` stands for any run of characters, and `?` for one."

  @doc "Returns whether `pattern` covers `log`."
  @spec covers?(String.t(), String.t()) :: boolean()
  def covers?(pattern, log), do: matches(pattern, log)

  @doc "Returns the org a pattern or log names, or `*` when its first segment is a wildcard."
  @spec org(String.t()) :: String.t()
  def org(pattern) do
    [first | _rest] = String.split(pattern, "/", parts: 2)
    if String.contains?(first, ["*", "?"]), do: "*", else: first
  end

  @doc "Returns the part of `pattern` before its first wildcard."
  @spec literal(String.t()) :: String.t()
  def literal(pattern) do
    [literal | _rest] = String.split(pattern, ["*", "?"], parts: 2)
    literal
  end

  defp matches("", ""), do: true
  defp matches("*" <> rest, log), do: matches(rest, log) or longer(rest, log)
  defp matches("?" <> rest, <<_char::utf8, log::binary>>), do: matches(rest, log)
  defp matches(<<char::utf8, rest::binary>>, <<char::utf8, log::binary>>), do: matches(rest, log)
  defp matches(_pattern, _log), do: false

  defp longer(rest, <<_char::utf8, log::binary>>), do: matches("*" <> rest, log)
  defp longer(_rest, ""), do: false
end
