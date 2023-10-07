defmodule Ecto.Adapters.SQLite.Supervisor do
  @moduledoc false
  use Supervisor

  @impl Supervisor
  def init(init_arg) do
    children = [
      {Ecto.Adapters.SQLite.Writer, init_arg},
      {Ecto.Adapters.SQLite.Reader, init_arg}
    ]

    # TODO
    {:ok}
  end
end
