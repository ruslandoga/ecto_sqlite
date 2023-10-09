defmodule EctoSQLite.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_sqlite,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:sqlite, path: "../sqlite.ex"},
      {:ecto_sql, "~> 3.10"}
    ]
  end
end
