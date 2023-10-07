defmodule Ecto.Adapters.SQLite.Storage do
  @moduledoc false

  def storage_up(opts) do
    database = Keyword.fetch!(opts, :database)
    File.touch!(database)
  end

  def storage_down(opts) do
    database = Keyword.fetch!(opts, :database)
    if File.exists?(database), do: File.rm!(database)

    wal = database <> "-wal"
    if File.exists?(wal), do: File.rm!(wal)

    shm = database <> "-shm"
    if File.exists?(shm), do: File.rm!(shm)

    :ok
  end

  def storage_status(opts) do
    database = Keyword.fetch!(opts, :database)
    if File.exists?(database), do: :up, else: :down
  end
end
