defmodule Memcache.Client.Serialization do

  defmodule Header do
    defstruct(magic: 0, opcode: 0, key_length: 0, extras_length: 0,
              data_type: 0, status: 0, total_body_length: 0, cas: 0)

    def length, do: 24
  end

  def encode_request(header, key, body \\ "", extras \\ "") do
    %Header{opcode: opcode, data_type: data_type, cas: cas} = header
    
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
  
  # opcodes

  def opcode(:get),        do: 0x00
  def opcode(:set),        do: 0x01
  def opcode(:add),        do: 0x02
  def opcode(:replace),    do: 0x03
  def opcode(:delete),     do: 0x04
  def opcode(:increment),  do: 0x05
  def opcode(:decrement),  do: 0x06
  def opcode(:quit),       do: 0x07
  def opcode(:flush),      do: 0x08
  def opcode(:getq),       do: 0x09
  def opcode(:noop),       do: 0x0A
  def opcode(:version),    do: 0x0B
  def opcode(:getk),       do: 0x0C
  def opcode(:getkq),      do: 0x0D
  def opcode(:append),     do: 0x0E
  def opcode(:prepend),    do: 0x0F
  def opcode(:stat),       do: 0x10
  def opcode(:setq),       do: 0x11
  def opcode(:addq),       do: 0x12
  def opcode(:replaceq),   do: 0x13
  def opcode(:deleteq),    do: 0x14
  def opcode(:incrementq), do: 0x15
  def opcode(:decrementq), do: 0x16
  def opcode(:quitq),      do: 0x17
  def opcode(:flushq),     do: 0x18
  def opcode(:appendq),    do: 0x19
  def opcode(:prependq),   do: 0x1A

  # SASL auth
  def opcode(:sasl_list_mechanisms), do: 0x20
  def opcode(:sasl_authenticate),    do: 0x21
  def opcode(:sasl_step),            do: 0x22
  
  def opcode_atom(0x00), do: :get
  def opcode_atom(0x01), do: :set
  def opcode_atom(0x02), do: :add
  def opcode_atom(0x03), do: :replace
  def opcode_atom(0x04), do: :delete
  def opcode_atom(0x05), do: :increment
  def opcode_atom(0x06), do: :decrement
  def opcode_atom(0x07), do: :quit
  def opcode_atom(0x08), do: :flush
  def opcode_atom(0x09), do: :getq
  def opcode_atom(0x0A), do: :noop
  def opcode_atom(0x0B), do: :version
  def opcode_atom(0x0C), do: :getk
  def opcode_atom(0x0D), do: :getkq
  def opcode_atom(0x0E), do: :append
  def opcode_atom(0x0F), do: :prepend
  def opcode_atom(0x10), do: :stat
  def opcode_atom(0x11), do: :setq
  def opcode_atom(0x12), do: :addq
  def opcode_atom(0x13), do: :replaceq
  def opcode_atom(0x14), do: :deleteq
  def opcode_atom(0x15), do: :incrementq
  def opcode_atom(0x16), do: :decrementq
  def opcode_atom(0x17), do: :quitq
  def opcode_atom(0x18), do: :flushq
  def opcode_atom(0x19), do: :appendq
  def opcode_atom(0x1A), do: :prependq

  # SASL auth
  def opcode_atom(0x20), do: :sasl_list_mechanisms
  def opcode_atom(0x21), do: :sasl_authenticate
  def opcode_atom(0x22), do: :sasl_step
  
  # status

  def status_atom(0x00), do: :ok
  def status_atom(0x01), do: :key_not_found
  def status_atom(0x02), do: :key_exists
  def status_atom(0x03), do: :value_too_large
  def status_atom(0x04), do: :invalid_arguments
  def status_atom(0x05), do: :item_not_stored
  def status_atom(0x06), do: :incr_or_decr_on_non_numeric_value
  def status_atom(0x20), do: :auth_failure
  def status_atom(0x21), do: :auth_continue
  def status_atom(_),    do: :unknown_status
  
end
