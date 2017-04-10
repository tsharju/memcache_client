defmodule Memcache.Client.WorkerTest do
  use ExUnit.Case
  alias Memcache.Client.Worker

  test "port settins as integer" do
    {:connect, :init, state} = Worker.init([host: "127.0.0.1", port: "11211", timeout: "5000"])
    assert %Memcache.Client.Worker.State{
      host: '127.0.0.1',
      port: 11211,
      auth_method: nil,
      from: nil,
      opts: nil,
      password: nil,
      socket: nil,
      timeout: 5000,
      username: nil
    } == state
  end
end
