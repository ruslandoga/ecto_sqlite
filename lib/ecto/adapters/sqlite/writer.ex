defmodule Ecto.Adapters.SQLite.Writer do
  @moduledoc false
  use GenServer

  @typep state :: %{
           db: reference,
           queue: :queue.queue({GenServer.from(), reference()}),
           writer: reference | nil
         }

  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @spec insert_all(pid | GenServer.name(), iodata, [[term]], timeout) ::
          :ok | {:error, SQLite.Error.t()}
  def insert_all(pid, sql, rows, timeout \\ 5000) do
    db = GenServer.call(pid, :out, timeout)

    try do
      SQLite.insert_all(db, sql, rows)
    after
      GenServer.cast(pid, :in)
    end
  end

  @spec query(pid | GenServer.name(), iodata, [term], timeout) ::
          {:ok, [[term]]} | {:error, SQLite.Error.t()}
  def query(pid, sql, args, timeout \\ 5000) do
    db = GenServer.call(pid, :out, timeout)

    try do
      SQLite.query(db, sql, args)
    after
      GenServer.cast(pid, :in)
    end
  end

  @impl true
  @spec init(Keyword.t()) :: {:ok, state} | {:error, SQLite.Error.t()}
  def init(config) do
    path = Keyword.fetch!(config, :path)
    flags = Keyword.fetch!(config, :flags)

    with {:ok, db} <- SQLite.open(path, flags),
         :ok <- SQLite.execute(db, "PRAGMA journal_mode=wal"),
         :ok <- SQLite.execute(db, "PRAGMA foreign_keys=on") do
      if after_connect = Keyword.get(config, :after_connect) do
        :ok = after_connect.(db)
      end

      {:ok, %{db: db, writer: nil, queue: :queue.new()}}
    end
  end

  @impl true
  @spec handle_call(:out, GenServer.from(), state) ::
          {:reply, reference, state} | {:noreply, state}
  def handle_call(:out, from, state) do
    %{db: db, writer: writer, queue: queue} = state
    {pid, _} = from
    ref = Process.monitor(pid)

    case writer do
      nil -> {:reply, db, %{state | writer: ref}}
      _pid -> {:noreply, %{state | queue: :queue.in({from, ref}, queue)}}
    end
  end

  @impl true
  @spec handle_cast(:in, state) :: {:noreply, state}
  def handle_cast(:in, state) do
    case :queue.out(state.queue) do
      {:empty, queue} ->
        {:noreply, %{state | writer: nil, queue: queue}}

      {{:value, {from, ref}}, queue} ->
        :ok = GenServer.reply(from, state.db)
        {:noreply, %{state | writer: ref, queue: queue}}
    end
  end

  @impl true
  @spec handle_info({:DOWN, reference, :process, pid, any}, state) :: {:noreply, state}
  def handle_info({:DOWN, ref, _, _, _}, %{writer: ref} = state) do
    Process.demonitor(ref, [:flush])

    case :queue.out(state.queue) do
      {:empty, queue} ->
        {:noreply, %{state | writer: nil, queue: queue}}

      {{:value, {from, ref}}, queue} ->
        :ok = GenServer.reply(from, state.db)
        {:noreply, %{state | writer: ref, queue: queue}}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    Process.demonitor(ref, [:flush])
    queue = :queue.delete_with(fn {_from, qref} -> ref == qref end, state.queue)
    {:noreply, %{state | queue: queue}}
  end
end
