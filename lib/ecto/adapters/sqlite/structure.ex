defmodule Ecto.Adapters.SQLite.Structure do
  @moduledoc false

  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    database = Keyword.fetch!(config, :database)
    flags = Keyword.fetch!(config, :flags)

    with {:ok, db} <- SQLite.open(database, flags),
         {:ok, queries} <- File.read(path) do
      multiquery_result =
        queries
        |> String.split(";", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> Enum.reduce_while({:ok, db}, fn
          query, {:ok, db} -> {:cont, SQLite.execute(db, query)}
          _query, {:error, _reason} = error -> {:halt, error}
        end)

      case multiquery_result do
        {:ok, _last_result, _conn} -> {:ok, path}
        {:error, reason} -> {:error, Exception.message(reason)}
      end
    end
  end

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"
    database = Keyword.fetch!(config, :database)
    flags = Keyword.fetch!(config, :flags)

    with {:ok, db} <- SQLite.open(database, flags),
         {:ok, sqlite_master} <-
           SQLite.fetch_all(db, "select sql from sqlite_master", [], 100),
         {:ok, versions} <- SQLite.fetch_all(db, "select * from #{migration_source}", [], 100) do
      sqlite_master = render_sqlite_master(sqlite_master)
      versions = render_versions(versions, migration_source)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, [sqlite_master, versions])
      {:ok, path}
    end
  end

  defp render_sqlite_master(sqlite_master) do
    Enum.map(sqlite_master, fn [sql] ->
      [sql |> String.trim() |> String.trim_trailing(";") |> String.trim_trailing(), ";\n"]
    end)
  end

  defp render_versions(versions, table) do
    Enum.map(versions, fn [version, inserted_at] ->
      ["INSERT INTO ", table, " (version, inserted_at) VALUES ('#{version}', #{inserted_at});\n"]
    end)
  end
end
