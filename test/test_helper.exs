# Logger.configure(level: :info)

# Application.put_env(:ecto, :primary_key_type, :id)
# Application.put_env(:ecto, :async_integration_tests, false)

# Code.require_file(
#   Path.join(Mix.Project.deps_paths().ecto, "integration_test/support/schemas.exs"),
#   __DIR__
# )

alias Ecto.Integration.TestRepo

database = Path.join(System.tmp_dir!(), "ecto_sqlite_test.db")

Application.put_env(:ecto_sqlite, TestRepo,
  adapter: Ecto.Adapters.SQLite,
  database: database,
  # pool: Ecto.Adapters.SQL.Sandbox,
  show_sensitive_data_on_connection_error: true
)

# defmodule Ecto.Integration.Case do
#   use ExUnit.CaseTemplate

#   alias Ecto.Adapters.SQL.Sandbox

#   setup do
#     :ok = Sandbox.checkout(TestRepo)
#     on_exit(fn -> Ecto.Adapters.SQL.Sandbox.checkin(TestRepo) end)
#   end
# end

{:ok, _} = Ecto.Adapters.SQLite.ensure_all_started(TestRepo.config(), :temporary)

# Load up the repository, start it, and run migrations
_ = Ecto.Adapters.SQLite.storage_down(TestRepo.config())
:ok = Ecto.Adapters.SQLite.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

:ok = Ecto.Migrator.up(TestRepo, 0, EctoSQLite.Integration.Migration, log: false)
# Ecto.Adapters.SQL.Sandbox.mode(TestRepo, :manual)
Process.flag(:trap_exit, true)

ExUnit.start()
