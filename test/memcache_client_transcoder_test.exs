defmodule Memcache.ClientTest.Transcoder do
  use ExUnit.Case

  test "raw transcoder encode_value" do
    assert {:error, {:invalid_value, %{}}} == Memcache.Client.Transcoder.encode_value(%{})

    {encoded, data_type} = Memcache.Client.Transcoder.encode_value("test")
    assert encoded == "test"
    assert data_type == 0x0000
  end

  test "raw transcoder decode_value" do
    assert "test" == Memcache.Client.Transcoder.decode_value("test", 0x0000)
    assert {:error, {:invalid_data_type, 2}} == Memcache.Client.Transcoder.decode_value("test", 0x0002)
  end
  
  test "json transcoder encode_value" do
    Application.put_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Json)
    
    {encoded, data_type} = Memcache.Client.Transcoder.encode_value(%{})

    assert encoded == "{}"
    assert data_type == 0x0002

    Application.delete_env(:memcache_client, :transcoder)
  end

  test "json transocder decode_value" do
    Application.put_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Json)
    
    assert %{} == Memcache.Client.Transcoder.decode_value("{}", 0x0002)
    assert {:error, {:invalid_data_type, 0}} == Memcache.Client.Transcoder.decode_value("{}", 0x0000)

    Application.delete_env(:memcache_client, :transcoder)
  end

  test "json transocder decode_value with opts" do
    Application.put_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Json)
    Application.put_env(:memcache_client, :transcoder_decode_opts, [keys: :atoms])
    
    assert %{test: "test"} == Memcache.Client.Transcoder.decode_value("{\"test\": \"test\"}", 0x0002)

    Application.delete_env(:memcache_client, :transcoder)
    Application.delete_env(:memcache_client, :transcoder_decode_opts)
  end

  test "erlang transcoder encode_value" do
    Application.put_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Erlang)
    
    {encoded, data_type} = Memcache.Client.Transcoder.encode_value(%{})

    assert encoded == <<131, 116, 0, 0, 0, 0>>
    assert data_type == 0x0004

    Application.delete_env(:memcache_client, :transcoder)
  end

  test "erlang transcoder decode_value" do
    Application.put_env(:memcache_client, :transcoder, Memcache.Client.Transcoder.Erlang)

    assert %{} == Memcache.Client.Transcoder.decode_value(<<131, 116, 0, 0, 0, 0>>, 0x0004)
    
    Application.delete_env(:memcache_client, :transcoder)
  end
  
end
