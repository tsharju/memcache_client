defmodule Memcache.Client do
  use Application

  alias Memcache.Client.Serialization.Header

  defmodule Response do
    defstruct value: "", extras: "", status: nil, cas: 0, type_flag: 0
  end
  
  def start(_type, _args) do
    import Supervisor.Spec, warn: false
    
    pool_args = [name: {:local, Memcache.Client.Pool},
                 worker_module: Memcache.Client.Worker,
                 size: Application.get_env(:memcache_client, :pool_size, 5),
                 max_overflow: Application.get_env(:memcache_client,
                                                   :pool_max_overflow, 10)]
    worker_args = [host: Application.get_env(:memcache_client, :host, "127.0.0.1"),
                   port: Application.get_env(:memcache_client, :port, 11211),
                   auth_method: Application.get_env(:memcache_client, :auth_method, :none),
                   username: Application.get_env(:memcache_client, :username, ""),
                   password: Application.get_env(:memcache_client, :password, "")]
    
    poolboy_sup = :poolboy.child_spec(Memcache.Client.Pool.Supervisor,
                                      pool_args, worker_args)
    
    children = [
      poolboy_sup
    ]
    
    opts = [strategy: :one_for_one, name: Memcache.Client.Supervisor]
    Supervisor.start_link(children, opts)
  end
  
  def get(key) do
    worker = :poolboy.checkout(Memcache.Client.Pool)
    header = %Header{opcode: :get}
    reply = GenServer.call(worker, {:request, header, key, "", ""})
    :poolboy.checkin(Memcache.Client.Pool, worker)
    
    case reply do
      {:ok, header, _key, body, extras} ->
        if header.status == :ok do
          <<type_flag :: size(32)>> = extras
          case Memcache.Client.Transcoder.decode_value(body, type_flag) do
            {:error, _error} ->
              header = %{header | status: :transcode_error}
              value = "Transcode error"
            value ->
              value = value
          end
        else
          value = body
        end
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
    header = %Header{opcode: opcode, cas: cas}
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
    header = %Header{opcode: :delete}
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
    header = %Header{opcode: opcode}
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
    header = %Header{opcode: :flush}
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
    header = %Header{opcode: :version}
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
