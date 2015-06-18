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
      end
    end
  end
  
  def run() do
    <<a::size(32), b::size(32), c::size(32)>> = :crypto.rand_bytes(12)
    :random.seed(a, b, c)
    
    IO.puts "Starting the Memcache.Client connection pool."
    IO.puts ""

    # configure connection pool size
    :ok = Application.put_env(:memcache_client, :pool_size, 10)
    
    {:ok, _started} = Application.ensure_all_started(:memcache_client)

    test_data_1k = unquote(test_data_1k)
    test_data_64k = unquote(test_data_64k)
    
    test_set(5, test_data_1k)
    test_set(5, test_data_64k)
    
    test_get(5)
    
    test_mset(5, test_data_1k)
    test_mset(5, test_data_64k)
  end
  
  defp test_set(num, data) do
    kbytes = :erlang.trunc(byte_size(data) / 1024)
    IO.puts "Starting #{num} processes. Doing 100000 set operations (#{kbytes} kB)."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            start = :erlang.now
            Enum.each(1..100000,
              fn i ->
                response = Memcache.Client.set("key#{idx}.#{i}", data)
                response.status == :ok
              end)
            took = :timer.now_diff(:erlang.now, start) / 1000000
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit
    
    count = Enum.count(results)
    min   = 100000 / Enum.max(results)
    max   = 100000 / Enum.min(results)
    sum   = Enum.sum(results)
    avg   = 100000 / (sum / count)

    IO.puts "max: #{max} min: #{min} avg: #{avg} sets/s"
    IO.puts ""
  end

  def test_mset(num, data) do
    kbytes = :erlang.trunc(byte_size(data) / 1024)
    IO.puts "Starting #{num} processes. Doing 100000 pipelined set operations (#{kbytes} kB)."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            keyvals = Enum.chunk(1..100000, 1000)
            |> Enum.map(
              fn chunk ->
                Enum.map(chunk, fn i -> {"key#{idx}.#{i}", data} end)
              end)
            start = :erlang.now
            
            Enum.each(keyvals,
              fn kvs ->
                [response] = Memcache.Client.mset(kvs) |> Enum.into([])
                response.status == :ok
              end)
            
            took = :timer.now_diff(:erlang.now, start) / 1000000
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit
    
    count = Enum.count(results)
    min   = 100000 / Enum.max(results)
    max   = 100000 / Enum.min(results)
    sum   = Enum.sum(results)
    avg   = 100000 / (sum / count)

    IO.puts "max: #{max} min: #{min} avg: #{avg} sets/s"
    IO.puts ""
  end
  
  def test_get(num) do
    IO.puts "Starting #{num} processes. Doing 100000 get operations."
    
    refs = Enum.map(
      1..num,
      fn idx ->
        Task.async(
          fn ->
            start = :erlang.now
            Enum.each(1..100000,
              fn i ->
                response = Memcache.Client.get("key#{idx}.#{i}")
                response.status == :ok
              end)
            took = :timer.now_diff(:erlang.now, start) / 1000000
            {:ok, took}
          end)
      end)
    |> Enum.into(HashSet.new, fn task -> task.ref end)

    results = do_receive(refs, []) # wait until all testers exit
    
    count = Enum.count(results)
    min   = 100000 / Enum.max(results)
    max   = 100000 / Enum.min(results)
    sum   = Enum.sum(results)
    avg   = 100000 / (sum / count)

    IO.puts "max: #{max} min: #{min} avg: #{avg} gets/s"
    IO.puts ""
  end
  
end
  
Memcache.Client.Benchmark.run()
