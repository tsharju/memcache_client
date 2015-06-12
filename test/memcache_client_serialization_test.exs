defmodule Memcache.ClientTest.Serialization do
  use ExUnit.Case

  alias Memcache.Client.Serialization

  test "encode request with only header and key" do
    header = %Serialization.Header{opcode: :get}
    
    bytes = Serialization.encode_request(header, "test")
    <<magic :: size(8), opcode :: size(8), _rest :: binary>> = bytes
    
    assert byte_size(bytes) == 28
    assert magic            == 0x80
    assert opcode           == 0x00
  end

  test "encode request with key and body" do
    header = %Serialization.Header{opcode: :get}

    bytes = Serialization.encode_request(header, "test", "test")    
    <<magic :: size(8), opcode :: size(8), _rest :: binary>> = bytes

    assert byte_size(bytes) == 32
    assert magic            == 0x80
    assert opcode           == 0x00
  end

  test "decode response header" do
    bytes = <<129, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    {:ok, header} = Serialization.decode_response_header(bytes)

    assert header.magic             == 0x81
    assert header.key_length        == 4
    assert header.total_body_length == 8
  end

  test "decode invalid response header wrong magic" do
    bytes = <<128, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    assert {:error, :invalid_header} == Serialization.decode_response_header(bytes)
  end
  
end
