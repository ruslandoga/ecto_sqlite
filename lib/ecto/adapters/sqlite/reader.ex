defmodule Ecto.Adapters.SQLite.Reader do
  @moduledoc false
  use GenServer

  @typep state :: %{
           db: reference,
           queue: nil | [{GenServer.from(), reference()}]
         }

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
  @spec init(Keyword.t()) :: {:ok, state} | {:error, SQLite.Error.t()}
  def init(config) do
    path = Keyword.fetch!(config, :path)
    flags = Keyword.fetch!(config, :flags)
    readonly_flags = flags |> Bitwise.bor(0b111) |> Bitwise.bxor(0b110)

    with {:ok, db} <- SQLite.open(path, readonly_flags),
         :ok = SQLite.set_update_hook(self(), [:command]) do
      if after_connect = Keyword.get(config, :after_connect) do
        # TODO expose SQLite.readonly?(db)
        :ok = after_connect.(db)
      end

      {:ok, %{db: db, queue: nil}}
    end
  end

  @impl true
  @spec handle_call(:out, GenServer.from(), state) ::
          {:reply, reference, state} | {:noreply, state}
  def handle_call(:out, from, state) do
    %{db: db, queue: queue} = state

    if queue do
      {pid, _} = from
      ref = Process.monitor(pid)
      {:noreply, %{state | queue: [{from, ref} | queue]}}
    else
      {:reply, db, state}
    end
  end

  @impl true
  @spec handle_cast(:in, state) :: {:noreply, state}
  def handle_cast(:in, state) do
    %{db: db, queue: queue} = state

    if queue && SQLite.get_autocommit(db) do
      Enum.each(queue, fn {from, ref} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, db)
      end)

      {:noreply, %{state | queue: nil}}
    else
      {:noreply, state}
    end
  end

  @impl true
  @spec handle_info({:DOWN, reference, :process, pid, any} | :insert | :update | :delete, state) ::
          {:noreply, state}
  def handle_info({:DOWN, _, _, _, _}, state) do
    %{db: db, queue: queue} = state

    if queue && SQLite.get_autocommit(db) do
      Enum.each(queue, fn {from, ref} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, db)
      end)

      {:noreply, %{state | queue: nil}}
    else
      {:noreply, state}
    end
  end

  def handle_info(cmd, state) when cmd in [:insert, :update, :delete] do
    if state.queue do
      {:noreply, state}
    else
      {:noreply, %{state | queue: []}}
    end
  end
end
