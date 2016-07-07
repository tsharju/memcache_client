defmodule Memcache.Client.Worker do

  use Connection

  alias Memcache.Client.Serialization.Opcode
  alias Memcache.Client.Serialization.Header
  alias Memcache.Client.Serialization

  @backoff_time 1000

  defmodule State do
    defstruct host: "127.0.0.1",
      port: 11211,
      opts: [],
      timeout: 5000,
      auth_method: :none,
      username: nil,
      password: nil,
      socket: nil,
      from: nil
  end

  def start_link(args), do: Connection.start_link(__MODULE__, args)

  def cast(worker, from, request, opcode) do
   header = %Header{opcode: opcode, cas: request.cas}
   Connection.cast(worker, {:request, from, header, request.key, request.value, request.extras})
  end

  def init(args) do
    host = case Keyword.get(args, :host) do
      host_arg when is_binary(host_arg) -> String.to_char_list(host_arg)
      host_arg -> host_arg
    end

    state = %State{
      host: host,
      port: Keyword.get(args, :port),
      opts: Keyword.get(args, :opts),
      timeout: Keyword.get(args, :timeout),
      auth_method: Keyword.get(args, :auth_method),
      username: Keyword.get(args, :username),
      password: Keyword.get(args, :password)
    }

    {:connect, :init, state}
  end

  def connect(_, %{socket: nil, host: host, port: port, opts: opts, timeout: timeout,
    username: username, password: password, auth_method: auth_method} = state) do
    case :gen_tcp.connect(host, port, opts, timeout) do
      {:ok, socket} ->
        cond do
          auth_method == :none ->
            {:ok, %{state | socket: socket}}
          auth_method == :sasl and sasl_authenticate(socket, username, password, timeout) == :ok ->
            {:ok, %{state | socket: socket}}
          true ->
            {:backoff, @backoff_time, state}
        end
      {:error, _} ->
        {:backoff, @backoff_time, state}
    end
  end

  def disconnect(info, %{socket: socket} = state) do
    :ok = :gen_tcp.close(socket)
    case info do
      {:error, :closed, from} ->
        Kernel.send(from, {:response, {:error, :closed}})
      {:error, reason} ->
        reason = :inet.format_error(reason)
        :error_logger.format("Connection error: ~s~n", [reason])
    end
    {:connect, :reconnect, %{state | socket: nil}}
  end

  def handle_cast({:request, from, _header, _key, _body, _extras}, %State{socket: nil} = state) do
    Kernel.send(from, {:response, {:error, :closed}})
    {:connect, :reconnect, state}
  end
  def handle_cast({:request, from, header, key, body, extras}, %State{socket: socket, timeout: timeout} = state) do
    bytes = Serialization.encode_request(header, key, body, extras)
    case :gen_tcp.send(socket, bytes) do
     :ok ->
      # start receiving if non quiet operation
      unless Opcode.quiet?(header.opcode), do: do_receive(socket, from, timeout)
      {:noreply, %{state | from: from}}
     {:error, reason} ->
       {:disconnect, {:error, reason, from}, state}
    end
  end

  # SASL authentication
  defp sasl_authenticate(socket, username, password, timeout) do
    bytes = Serialization.encode_request(%Header{opcode: :sasl_list_mechanisms}, "", "", "")
    {:ok, header, _key, body, _extras} = send_and_receive(socket, bytes, timeout)
    case header.status do
      :ok ->
        String.split(body, " ")
        |> hd
        |> do_sasl_auth(username, password, socket, timeout)
      reason ->
        reason
    end
  end

  defp do_sasl_auth("PLAIN", username, password, socket, timeout) do
    bytes = Serialization.encode_request(
      %Header{opcode: :sasl_authenticate},
      "PLAIN", "#{username}\0#{username}\0#{password}", "")
    {:ok, header, _key, _body, _extras} = send_and_receive(socket, bytes, timeout)
      header.status
  end
  defp do_sasl_auth(_, _, _, _, _), do: {:error, :unknown_sasl_auth_mechanism}

  defp do_receive(socket, nil, timeout) do
    case receive_header(socket, timeout) do
      {:ok, header} ->
          case receive_body(socket, header.total_body_length, timeout) do
            {:ok, total_body} ->
               case Serialization.decode_response_body(header, total_body) do
                 {:ok, key, body, extras} ->
                   {:ok, header, key, body, extras}
                 error -> error
               end
            error -> error
          end
      error -> error
    end
  end
  defp do_receive(socket, reply_to, timeout) do
    case do_receive(socket, nil, timeout) do
      {:ok, header, _key, _body, _extras} = response ->
        Kernel.send(reply_to, {:response, response})
        if Opcode.quiet?(header.opcode), do: do_receive(socket, reply_to, timeout)
      {:error, _reason} = error ->
        Kernel.send(reply_to, {:response, error})
    end
  end

  defp receive_header(socket, timeout) do
    case :gen_tcp.recv(socket, Header.length, timeout) do
      {:ok, bytes} ->
        Serialization.decode_response_header(bytes)
      error ->
        error
    end
  end

  defp receive_body(_socket, 0, _timeout), do: {:ok, ""}
  defp receive_body(socket, total_body_length, timeout) when total_body_length > 0 do
    :gen_tcp.recv(socket, total_body_length, timeout)
  end

  defp send_and_receive(socket, bytes, timeout) do
    :ok = :gen_tcp.send(socket, bytes)
    do_receive(socket, nil, timeout)
  end

end
