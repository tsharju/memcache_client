#!/usr/bin/env elixir -pa _build/prod/lib/*/ebin

# NOTE: remember to run "MIX_ENV=prod mix compile" before running benchmark

defmodule Memcache.Client.Benchmark do

  test_data_1k = Enum.reduce(1..1024, "", fn _, acc -> "a" <> acc end)
  test_data_64k = Enum.reduce(1..1024*64, "", fn _, acc -> "a" <> acc end)
  
  def do_receive(refs, acc) do
    if HashSet.size(refs) == 0 do
      acc
    else
      receive do
        {_ref, {:ok, time}} ->
          do_receive(refs, [time | acc])
        {:'DOWN', ref, _, _, _} ->
          refs = HashSet.delete(refs, ref)
          do_receive(refs, acc)
        msg ->
          IO.puts "Uncaught message: #{inspect msg}"
      end
    end
  end
  
  def run() do
    <<a::size(32), b::size(32), c::size(32)>> = :crypto.rand_bytes(12)
    :random.seed(a, b, c)
    
    IO.puts "Starting the Memcache.Client connection pool."
    IO.puts ""

    # configure connection pool size
    :ok = Application.put_env(:memcache_client, :pool_size, 5)
    
    {:ok, _started} = Application.ensure_all_started(:memcache_client)

    test_data_1k = unquote(test_data_1k)
    test_data_64k = unquote(test_data_64k)
    
    test_set(100, 100, test_data_1k)
    test_set(100, 100, test_data_64k)
    
    test_get(100, 100)
    
    test_mset(100, 100, test_data_1k)
    test_mset(100, 100, test_data_64k)
  end
  
  defp test_set(num, num_ops, data) do
    kbytes = :erlang.trunc(byte_size(data) / 1024)
    IO.puts "Starting #{num} processes. Doing #{num_ops} set operations (#{kbytes} kB)."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            start = :erlang.timestamp
            Enum.each(1..num_ops,
              fn i ->
                response = Memcache.Client.set("key#{idx}.#{i}", data)
                response.status == :ok
              end)
            took = (:timer.now_diff(:erlang.timestamp, start) / 1000000) / num_ops
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit
    
    count   = Enum.count(results)
    min     = Enum.min(results)
    max     = Enum.max(results)
    sum     = Enum.sum(results)
    avg     = (sum / count)
    per_sec = 1 / avg
    
    IO.puts "max: #{max} min: #{min} avg: #{avg} #{per_sec} sets/s"
    IO.puts ""
  end

  def test_mset(num, num_ops, data) do
    kbytes = :erlang.trunc(byte_size(data) / 1024)
    IO.puts "Starting #{num} processes. Doing #{num_ops} pipelined set operations (#{kbytes} kB)."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            keyvals = Enum.chunk(1..num_ops, 10)
            |> Enum.map(
              fn chunk ->
                Enum.map(chunk, fn i -> {"key#{idx}.#{i}", data} end)
              end)
            start = :erlang.timestamp
            
            Enum.each(keyvals,
              fn kvs ->
                [response] = Memcache.Client.mset(kvs) |> Enum.into([])
                response.status == :ok
              end)
            
            took = (:timer.now_diff(:erlang.timestamp, start) / 1000000) / num_ops
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit

    count   = Enum.count(results)
    min     = Enum.min(results)
    max     = Enum.max(results)
    sum     = Enum.sum(results)
    avg     = (sum / count)
    per_sec = 1 / avg

    IO.puts "max: #{max} min: #{min} avg: #{avg} #{per_sec} sets/s"
    IO.puts ""
  end
  
  def test_get(num, num_ops) do
    IO.puts "Starting #{num} processes. Doing #{num_ops} get operations."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            start = :erlang.timestamp
            Enum.each(1..num_ops,
              fn i ->
                response = Memcache.Client.get("key#{idx}.#{i}")
                response.status == :ok
              end)
            took = (:timer.now_diff(:erlang.timestamp, start) / 1000000) / num_ops
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit
    
    count   = Enum.count(results)
    min     = Enum.min(results)
    max     = Enum.max(results)
    sum     = Enum.sum(results)
    avg     = (sum / count)
    per_sec = 1 / avg

    IO.puts "max: #{max} min: #{min} avg: #{avg} #{per_sec} gets/s"
    IO.puts ""
  end
  
end
  
Memcache.Client.Benchmark.run()
