Memcache.Client
===============

[![Build Status](https://travis-ci.org/tsharju/memcache_client.svg)](https://travis-ci.org/tsharju/memcache_client)

`Memcache.Client` is a memcached client library utilizing the memcached binary protocol.

Installing
----------

You can install `Memcache.Client` by adding it as a dependecy to your
project's `mix.exs` file:

```elixir
defp deps do
  [
    {:memcache_client, "~> 1.0.0"}
  ]
end
```

Also, remember to add `:memcache_client` to your `:applications` list
if you wish that the application is started automatically.

Examples
--------

### Get value for a key:

```elixir
response = Memcache.Client.get("key")
case response.status do
  :ok ->
    {:ok, response.value}
  status ->
    {:error, status}
end
```

### Get values for multiple keys with a single operation:

```elixir
responses = Memcache.Client.mget(["key1", "key2", "key3"]) |> Enum.into([])
```

As you can see, the multi get operation returns a stream that needs to
be consumed (for example using `Enum.into`) in order to receive the
responses.

### Set value for a key:

```elixir
response = Memcache.Client.set("key", "value")
case response.status do
  :ok ->
    {:ok, response.cas}
  status ->
    {:error, status}
end
```

You can also do a multiset operation similar to the multiget above.

### Transcoders

`Memcache.Client` also supports transcoders for serializing and
deserializing the data. The default transcoder
`Memcache.Client.Transcoder.Raw` expects the value to be a
binary. This transoder does not do anything but check that when value
is set that it actually is a binary.

Two other transcoders are included. Namely
`Memcache.Client.Transcoder.Erlang` and
`Memcache.Client.Transcoder.Json`. The first one uses Erlang's
`term_to_binary` for converting Elixir terms to binary and when
returning them they are automatically converted back. The JSON
transcoder uses `Poison` JSON library for serializing and
deserializing the data to and from JSON.

You can implement also your own transcoder by implementing the
`Memcache.Client.Transcoder` behaviour.

You can set the transcoder by setting the config value `:transcoder`
for the `:memcache_client` application.

TODO
----

* Support for consistent hashing.
