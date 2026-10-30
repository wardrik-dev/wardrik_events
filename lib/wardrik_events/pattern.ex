defmodule WardrikEvents.Pattern do
  @moduledoc """
  Matches log names against patterns. `*` matches any run of characters, and `?` matches one.

      WardrikEvents.Pattern.covers?("acme/*", "acme/triage")
      #=> true
  """

  @doc "Returns `true` if `pattern` matches `log`."
  @spec covers?(String.t(), String.t()) :: boolean()
  def covers?(pattern, log), do: matches(pattern, log)

  @doc "Returns the org, the first segment of a pattern or log. Returns `\"*\"` if that segment has a wildcard."
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
