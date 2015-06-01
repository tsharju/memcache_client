defmodule Memcache.Client.Worker do
  use GenServer
  @behaviour :poolboy_worker

  @_RECEIVE_TIMEOUT 5000
  
  alias Memcache.Client.Serialization

  def start_link(args) do
    GenServer.start_link(__MODULE__, [args], [])
  end
  
  def init(args) do
    host = Keyword.get(args, :host, "127.0.0.1")
    port = Keyword.get(args, :port, 11211)
    
    socket_opts = [:binary, {:nodelay, true}, {:active, false}, {:packet, 0}]
    {:ok, socket} = :gen_tcp.connect(String.to_char_list(host), port, socket_opts)
    
    {:ok, %{socket: socket}}
  end
  
  def handle_call({:request, header, key, body, extras}, _from, %{socket: socket} = state) do
    bytes = Serialization.encode_request(header, key, body, extras)
    case :gen_tcp.send(socket, bytes) do
      {:error, reason} ->
        {:stop, reason, {:error, reason}, state}
      :ok ->
        case receive_header(socket) do
          {:ok, header} ->
            case receive_body(socket, header.total_body_length) do
              {:ok, total_body} ->
                key_length = header.key_length
                extras_length = header.extras_length
                body_length = (header.total_body_length - (key_length + extras_length))
                <<extras :: binary-size(extras_length), key :: binary-size(key_length),
                body :: binary-size(body_length)>> = total_body
                {:reply, {:ok, header, key, body, extras}, state}
              {:error, reason} ->
                {:stop, reason, {:error, reason}, state}
            end
          {:error, reason} ->
            {:stop, reason, {:error, reason}, state}
        end
    end
  end
  
  defp receive_header(socket) do
    case :gen_tcp.recv(socket, Serialization.Header.length, @_RECEIVE_TIMEOUT) do
      {:ok, bytes} ->
        {:ok, header} = Serialization.decode_response_header(bytes)
        {:ok, header}
      error ->
        error
    end
  end
  
  defp receive_body(_socket, 0), do: {:ok, ""}
  defp receive_body(socket, total_body_length) when total_body_length > 0 do
    case :gen_tcp.recv(socket, total_body_length, @_RECEIVE_TIMEOUT) do
      {:ok, body} ->
        {:ok, body}
      error ->
        error
    end
  end
  
end
