defmodule EctoSQLite.Integration.Migration do
  @moduledoc false
  use Ecto.Migration

  def change do
    create table(:accounts) do
      add :name, :string
      add :email, :string, collate: :nocase
      timestamps()
    end
  end
end
