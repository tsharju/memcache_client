defmodule Memcache.Client do
  use Application

  alias Memcache.Client.Serialization

  defmodule Response do
    defstruct value: "", extras: "", status: nil, cas: 0, type_flag: 0
  end
  
  def start(_type, _args) do
    import Supervisor.Spec, warn: false
    
    pool_args = [name: {:local, Memcache.Client.Pool},
                 worker_module: Memcache.Client.Worker,
                 size: 10,
                 max_overflow: 20]
    worker_args = [host: "127.0.0.1",
                   port: 11211]
    
    poolboy_sup = :poolboy.child_spec(Memcache.Client.Pool,
                                      pool_args, worker_args)
    
    children = [
      poolboy_sup
    ]
    
    opts = [strategy: :one_for_one, name: Memcache.Client.Supervisor]
    Supervisor.start_link(children, opts)
  end
  
  def get(key) do
    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode :get}
    reply = GenServer.call(worker, {:request, header, key, "", ""})
    :poolboy.checkin(Memcache.Client.Pool, worker)
    
    case reply do
      {:ok, header, _key, body, extras} ->
        <<type_flag :: size(32)>> = extras
        value = Memcache.Client.Transcoder.decode_value(body, type_flag)
        %Response{value: value, extras: extras, status: header.status,
                  cas: header.cas, type_flag: type_flag}
      error ->
        error
    end
  end
  
  def set(key, value, opts \\ []), do: do_store(:set, key, value, opts)
  
  def add(key, value, opts \\ []), do: do_store(:add, key, value, opts)
  
  def replace(key, value, opts \\ []), do: do_store(:replace, key, value, opts)
  
  defp do_store(opcode, key, value, opts) do
    expires = Keyword.get(opts, :expires, 0)
    cas     = Keyword.get(opts, :cas, 0)

    {value, flags} = Memcache.Client.Transcoder.encode_value(value)
    extras = <<flags :: size(32), expires :: size(32)>>
    
    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode(opcode), cas: cas}
    reply = GenServer.call(worker, {:request, header, key, value, extras})
    :poolboy.checkin(Memcache.Client.Pool, worker)
    
    case reply do
      {:ok, header, _key, body, extras} ->
        %Response{value: body, extras: extras, status: header.status, cas: header.cas}
      error ->
        error
    end
  end

  def delete(key) do
    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode :delete}
    reply = GenServer.call(worker, {:request, header, key, "", ""})
    :poolboy.checkin(Memcache.Client.Pool, worker)
    
    case reply do
      {:ok, header, _key, body, extras} ->
        %Response{value: body, extras: extras, status: header.status, cas: header.cas}
      error ->
        error
    end
  end

  def increment(key, amount, opts \\ []), do: do_incr_decr(:increment, key, amount, opts)
  
  def decrement(key, amount, opts \\ []), do: do_incr_decr(:decrement, key, amount, opts)
  
  defp do_incr_decr(opcode, key, amount, opts) do
    initial_value = Keyword.get(opts, :initial_value, 0)
    expires       = Keyword.get(opts, :expires, 0)
    
    extras = <<amount :: size(64), initial_value :: size(64), expires :: size(32)>>

    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode(opcode)}
    reply = GenServer.call(worker, {:request, header, key, "", extras})
    :poolboy.checkin(Memcache.Client.Pool, worker)
    
    case reply do
      {:ok, header, _key, body, extras} ->
        if header.status == :ok do
          <<body :: unsigned-integer-size(64)>> = body
        end
        %Response{value: body, extras: extras, status: header.status, cas: header.cas}
      error ->
        error
    end
  end

  def flush(opts \\ []) do
    expires = Keyword.get(opts, :expires, 0)

    extras = <<expires :: size(32)>>

    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode :flush}
    reply = GenServer.call(worker, {:request, header, "", "", extras})
    :poolboy.checkin(Memcache.Client.Pool, worker)

    case reply do
      {:ok, header, _key, body, extras} ->
        %Response{value: body, extras: extras, status: header.status, cas: header.cas}
      error ->
        error
    end
  end

  def version() do
    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Serialization.Header{opcode: Serialization.opcode :version}
    reply = GenServer.call(worker, {:request, header, "", "", ""})
    :poolboy.checkin(Memcache.Client.Pool, worker)

    case reply do
      {:ok, header, _key, body, extras} ->
        %Response{value: body, extras: extras, status: header.status, cas: header.cas}
      error ->
        error
    end
  end
  
end
