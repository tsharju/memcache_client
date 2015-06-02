defmodule Memcache.Client.Transcoder do

  use Behaviour
  
  defcallback encode_value(value :: any) :: {binary, binary}
  defcallback decode_value(value :: binary, data_type :: binary) :: any

  def encode_value(value) do
    transcoder = Application.get_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Raw)
    transcoder.encode_value(value)
  end

  def decode_value(value, data_type) do
    transcoder = Application.get_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Raw)
    transcoder.decode_value(value, data_type)
  end
  
end

defmodule Memcache.Client.Transcoder.Erlang do
  @behaviour Memcache.Client.Transcoder

  @erlang_type_flag 0x0004

  def encode_value(value) do
    {:erlang.term_to_binary(value), @erlang_type_flag}
  end

  def decode_value(value, @erlang_type_flag) do
    :erlang.binary_to_term(value)
  end
  def decode_value(_value, data_type), do: {:error, {:invalid_data_type, data_type}}
  
end

defmodule Memcache.Client.Transcoder.Raw do
  @behaviour Memcache.Client.Transcoder

  @raw_type_flag 0x0000

  def encode_value(value) when is_binary(value) do
    {value, @raw_type_flag}
  end
  def encode_value(value), do: {:error, {:invalid_value, value}}

  def decode_value(value, @raw_type_flag) do
    value
  end
  def decode_value(_value, data_type), do: {:error, {:invalid_data_type, data_type}}
  
end

defmodule Memcache.Client.Transcoder.Json do
  @behaviour Memcache.Client.Transcoder

  @json_type_flag 0x0002
  
  def encode_value(value) do
    opts = Application.get_env(:memcache_client, :transcoder_encode_opts, [])
    
    case Poison.encode(value, opts) do
      {:ok, data} ->
        {data, @json_type_flag}
      error ->
        error
    end
  end

  def decode_value(value, @json_type_flag) do
    opts = Application.get_env(:memcache_client, :transcoder_decode_opts, [])
    {:ok, value} = Poison.decode(value, opts)
    value
  end
  def decode_value(_value, data_type), do: {:error, {:invalid_data_type, data_type}}
  
end
