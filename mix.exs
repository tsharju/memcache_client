defmodule Memcache.Client.Mixfile do
  use Mix.Project

  def project do
    [app: :memcache_client,
     version: "1.0.0",
     elixir: "~> 1.0",
     description: description,
     package: package,
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger, :poolboy],
     mod: {Memcache.Client, []}]
  end

  defp deps do
    [
      {:earmark, "~> 0.2.0", only: :dev},
      {:ex_doc, "~> 0.11.4", only: :dev},
      {:poison, "~> 2.1.0"},
      {:poolboy, "~> 1.5.1"},
      {:connection, "~> 1.0.2"}
    ]
  end

  defp description do
    """
    Memcache client library utilizing the memcache binary protocol.
    """
  end

  defp package do
    [
      maintainers: ["Teemu Harju"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/tsharju/memcache_client"}
    ]
  end
  
end
