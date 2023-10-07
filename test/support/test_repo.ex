defmodule Ecto.Integration.TestRepo do
  @moduledoc false
  use Ecto.Repo,
    otp_app: :ecto_sqlite,
    adapter: Ecto.Adapters.SQLite

  def create_prefix(_) do
    raise ArgumentError, "SQLite3 does not support CREATE DATABASE"
  end

  def drop_prefix(_) do
    raise ArgumentError, "SQLite3 does not support DROP DATABASE"
  end

  def uuid, do: Ecto.UUID
end
