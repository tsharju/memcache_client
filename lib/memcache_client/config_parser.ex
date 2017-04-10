defmodule Memcache.Client.ConfigParser do
  def to_binary(arg) do
    case arg do
      value when is_binary(value) -> String.to_char_list(value)
      value -> value
    end
  end

  def to_integer(arg) do
    case arg do
      nil -> nil
      value when is_integer(value) -> value
      value ->
        case Integer.parse(value) do
          {int, _} -> int
          :error -> value
        end
    end
  end
end
