defmodule Memcache.Client.Worker do
  use GenServer
  @behaviour :poolboy_worker

  @_RECEIVE_TIMEOUT 5000

  @initial_state %{socket: nil, from: nil}

  alias Memcache.Client.Serialization.Opcode
  alias Memcache.Client.Serialization.Header
  alias Memcache.Client.Serialization

  def start_link(args) do
    GenServer.start_link(__MODULE__, [args], [])
  end
  
  def init([args]) do
    host        = Keyword.get(args, :host, "127.0.0.1")
    port        = Keyword.get(args, :port, 11211)
    auth_method = Keyword.get(args, :auth_method, :none)
    
    socket_opts = [:binary, {:nodelay, true}, {:active, false}, {:packet, :raw}]
    {:ok, socket} = :gen_tcp.connect(String.to_char_list(host), port, socket_opts)
    
    case auth_method do
      :sasl ->
        username = Keyword.get(args, :username, "")
        password = Keyword.get(args, :password, "")
        
        case sasl_authenticate(socket, username, password) do
          :ok ->
            {:ok, %{@initial_state | socket: socket}}
          error ->
            error
        end
      :none ->
        {:ok, %{@initial_state | socket: socket}}
    end
  end
  
  def handle_cast({:request, from, header, key, body, extras}, %{socket: socket} = state) do
    bytes = Serialization.encode_request(header, key, body, extras)
    :gen_tcp.send(socket, bytes)
    
    # start receiving if non quiet operation
    if not Opcode.quiet?(header.opcode) do
      do_receive(socket, from)
    end
    
    {:noreply, %{state | from: from}}
  end

  def terminate(reason, %{socket: socket} = state) do
    :gen_tcp.close(socket)
  end
  
  defp do_receive(socket, reply_to) do
    case receive_header(socket) do
      {:ok, header} ->
        case receive_body(socket, header.total_body_length) do
          {:ok, total_body} ->
            {:ok, key, body, extras} = Serialization.decode_response_body(header, total_body)
            if reply_to != nil do
              # if we have pid we send the response there
              Kernel.send(reply_to, {:response, {:ok, header, key, body, extras}})
              # read until non quiet opcode
              if Opcode.quiet?(header.opcode) do
                do_receive(socket, reply_to)
              end
            else
              # just return the response
              {:ok, header, key, body, extras}
            end
          {:error, reason} when reply_to != nil ->
            Kernel.send(reply_to, {:error, reason})
          {:error, reason} ->
            {:error, reason}
        end
      {:error, reason} when reply_to != nil ->
        Kernel.send(reply_to, {:error, reason})
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  defp receive_header(socket) do
    case :gen_tcp.recv(socket, Header.length, @_RECEIVE_TIMEOUT) do
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

  defp send_and_receive(socket, bytes) do
    :ok = :gen_tcp.send(socket, bytes)
    do_receive(socket, nil)    
  end

  # SASL authentication

  defp sasl_authenticate(socket, username, password) do
    bytes = Serialization.encode_request(
      %Header{opcode: :sasl_list_mechanisms}, "", "", "")
    {:ok, header, _key, body, _extras} = send_and_receive(socket, bytes)

    case header.status do
      :ok ->
        mechanisms = String.split(body, " ")
        do_sasl_auth(mechanisms, username, password, socket)
      reason ->
        {:error, reason}
    end
  end

  defp do_sasl_auth([mechanism | _mechanisms], username, password, socket) do
    do_sasl_auth(mechanism, username, password, socket)
  end

  defp do_sasl_auth("PLAIN", username, password, socket) do
    bytes = Serialization.encode_request(
      %Header{opcode: :sasl_authenticate},
      "PLAIN", "#{username}\0#{username}\0#{password}", "")
    {:ok, header, _key, _body, _extras} = send_and_receive(socket, bytes)
    case header.status do
      :ok ->
        :ok
      reason ->
        {:error, reason}
    end
  end
  defp do_sasl_auth(_, _, _, _), do: {:error, :unknown_sasl_auth_mechanism}
  
end
