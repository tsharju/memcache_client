defmodule Memcache.Client.Serialization do

  defmodule Header do
    defstruct(magic: 0, opcode: 0, key_length: 0, extras_length: 0,
              data_type: 0, status: 0, total_body_length: 0, cas: 0)

    def length, do: 24
  end

  def encode_request(header, key, body \\ "", extras \\ "") do
    %Header{opcode: opcode, data_type: data_type, cas: cas} = header

    opcode = opcode_byte(opcode)
    key_length = byte_size(key)
    extras_length = byte_size(extras)
    
    total_body = <<extras :: binary, key :: binary, body :: binary>>
    total_body_length = byte_size(total_body)
    
    <<0x80 :: size(8), opcode :: size(8), key_length :: size(16),
    extras_length :: size(8), data_type :: size(8), 0 :: size(16),
    total_body_length :: size(32), 0 :: size(32), cas :: size(64),
    total_body :: binary>>
  end

  def decode_response_header(<<0x81 :: size(8), opcode :: size(8),
                             key_length :: size(16), extras_length :: size(8),
                             data_type :: size(8), status :: size(16),
                             total_body_length :: size(32), _opaque :: size(32),
                             cas :: size(64)>>) do
    {:ok, %Header{magic: 0x81, opcode: opcode_atom(opcode), key_length: key_length,
                  extras_length: extras_length, data_type: data_type,
                  status: status_atom(status), total_body_length: total_body_length,
                  cas: cas}}
  end
  def decode_response_header(_bytes), do: {:error, :invalid_header}

  def decode_response_body(header, total_body) do
    key_length = header.key_length
    extras_length = header.extras_length
    body_length = (header.total_body_length - (key_length + extras_length))
    <<extras :: binary-size(extras_length), key :: binary-size(key_length),
    body :: binary-size(body_length)>> = total_body
    {:ok, key, body, extras}
  end
  
  # opcodes

  defp opcode_byte(:get),        do: 0x00
  defp opcode_byte(:set),        do: 0x01
  defp opcode_byte(:add),        do: 0x02
  defp opcode_byte(:replace),    do: 0x03
  defp opcode_byte(:delete),     do: 0x04
  defp opcode_byte(:increment),  do: 0x05
  defp opcode_byte(:decrement),  do: 0x06
  defp opcode_byte(:quit),       do: 0x07
  defp opcode_byte(:flush),      do: 0x08
  defp opcode_byte(:getq),       do: 0x09
  defp opcode_byte(:noop),       do: 0x0A
  defp opcode_byte(:version),    do: 0x0B
  defp opcode_byte(:getk),       do: 0x0C
  defp opcode_byte(:getkq),      do: 0x0D
  defp opcode_byte(:append),     do: 0x0E
  defp opcode_byte(:prepend),    do: 0x0F
  defp opcode_byte(:stat),       do: 0x10
  defp opcode_byte(:setq),       do: 0x11
  defp opcode_byte(:addq),       do: 0x12
  defp opcode_byte(:replaceq),   do: 0x13
  defp opcode_byte(:deleteq),    do: 0x14
  defp opcode_byte(:incrementq), do: 0x15
  defp opcode_byte(:decrementq), do: 0x16
  defp opcode_byte(:quitq),      do: 0x17
  defp opcode_byte(:flushq),     do: 0x18
  defp opcode_byte(:appendq),    do: 0x19
  defp opcode_byte(:prependq),   do: 0x1A

  # SASL auth
  defp opcode_byte(:sasl_list_mechanisms), do: 0x20
  defp opcode_byte(:sasl_authenticate),    do: 0x21
  defp opcode_byte(:sasl_step),            do: 0x22
  
  defp opcode_atom(0x00), do: :get
  defp opcode_atom(0x01), do: :set
  defp opcode_atom(0x02), do: :add
  defp opcode_atom(0x03), do: :replace
  defp opcode_atom(0x04), do: :delete
  defp opcode_atom(0x05), do: :increment
  defp opcode_atom(0x06), do: :decrement
  defp opcode_atom(0x07), do: :quit
  defp opcode_atom(0x08), do: :flush
  defp opcode_atom(0x09), do: :getq
  defp opcode_atom(0x0A), do: :noop
  defp opcode_atom(0x0B), do: :version
  defp opcode_atom(0x0C), do: :getk
  defp opcode_atom(0x0D), do: :getkq
  defp opcode_atom(0x0E), do: :append
  defp opcode_atom(0x0F), do: :prepend
  defp opcode_atom(0x10), do: :stat
  defp opcode_atom(0x11), do: :setq
  defp opcode_atom(0x12), do: :addq
  defp opcode_atom(0x13), do: :replaceq
  defp opcode_atom(0x14), do: :deleteq
  defp opcode_atom(0x15), do: :incrementq
  defp opcode_atom(0x16), do: :decrementq
  defp opcode_atom(0x17), do: :quitq
  defp opcode_atom(0x18), do: :flushq
  defp opcode_atom(0x19), do: :appendq
  defp opcode_atom(0x1A), do: :prependq

  # SASL auth
  defp opcode_atom(0x20), do: :sasl_list_mechanisms
  defp opcode_atom(0x21), do: :sasl_authenticate
  defp opcode_atom(0x22), do: :sasl_step
  
  # status

  defp status_atom(0x00), do: :ok
  defp status_atom(0x01), do: :key_not_found
  defp status_atom(0x02), do: :key_exists
  defp status_atom(0x03), do: :value_too_large
  defp status_atom(0x04), do: :invalid_arguments
  defp status_atom(0x05), do: :item_not_stored
  defp status_atom(0x06), do: :incr_or_decr_on_non_numeric_value
  defp status_atom(0x20), do: :auth_failure
  defp status_atom(0x21), do: :auth_continue
  defp status_atom(_),    do: :unknown_status
  
end
