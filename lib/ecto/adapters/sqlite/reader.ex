defmodule Ecto.Adapters.SQLite.Reader do
  @moduledoc false
  use GenServer

  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @spec fetch_all(GenServer.name() | pid, iodata, [term], timeout) ::
          {:ok, [[term]]} | {:error, SQLite.Error.t()}
  def fetch_all(pid, sql, args, timeout \\ 5000) do
    db = GenServer.call(pid, :out, timeout)

    try do
      SQLite.fetch_all(db, sql, args, 100)
    after
      GenServer.cast(pid, :in)
    end
  end

  @impl true
  def init(config) do
    path = Keyword.fetch!(config, :path)
    flags = Keyword.fetch!(config, :flags)
    flags = Bitwise.band(flags, _readonly = 0x1)

    with {:ok, db} <- SQLite.open(path, flags), :ok = SQLite.set_update_hook(self()) do
      if after_connect = Keyword.get(config, :after_connect) do
        # TODO expose SQLite.readonly?(db)
        :ok = after_connect.(db)
      end

      {:ok, %{db: db, queue: nil}}
    end
  end

  @impl true
  def handle_call(:out, from, state) do
    %{db: db, queue: queue} = state

    case queue do
      nil ->
        {:reply, db, state}

      _queue ->
        {pid, _} = from
        ref = Process.monitor(pid)
        {:noreply, %{state | queue: :queue.in({from, ref}, queue)}}
    end
  end

  @impl true
  def handle_cast(:in, state) do
    %{db: db, queue: queue} = state

    case queue do
      nil ->
        {:noreply, state}

      queue ->
        case SQLite.get_autocommit(db) do
          true -> {:noreply, %{state | queue: nil}}
          false -> {:noreply, state}
        end
    end
  end

  # @impl true
  # def handle_info()
end
