defmodule Ecto.Adapters.SQLite do
  @moduledoc """
  TODO
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Transaction
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @conn __MODULE__.Connection

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
    quote do
    end
  end

  @impl Ecto.Adapter
  def ensure_all_started(config, type) do
    IO.inspect([config: config, type: type], label: "ensure_all_started")
    {:ok, []}
  end

  @impl Ecto.Adapter
  def init(config) do
    # Ecto.Adapters.SQL.init(@conn, @driver, config)
    IO.inspect([config: config], label: "init")
    {:ok, Ecto.Adapters.SQLite.RWQueue, _meta = %{}}
  end

  @impl Ecto.Adapter
  def checkout(_meta, _opts, _fun) do
    # Ecto.Adapters.SQL.checkout(meta, opts, fun)
    raise "todo"
  end

  @impl Ecto.Adapter
  def checked_out?(_meta) do
    # Ecto.Adapters.SQL.checked_out?(meta)
    raise "todo"
  end

  @impl Ecto.Adapter
  def loaders({:map, _}, type), do: [&Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_, type), do: [type]

  @impl Ecto.Adapter
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_, type), do: [type]

  @impl Ecto.Adapter.Queryable
  def prepare(:all, query), do: {:nocache, @conn.all(query)}
  def prepare(:update_all, query), do: {:nocache, @conn.update_all(query)}
  def prepare(:delete_all, query), do: {:nocache, @conn.delete_all(query)}

  @impl Ecto.Adapter.Queryable
  def execute(_adapter_meta, _query_meta, _query, _params, _opts) do
    # Ecto.Adapters.SQL.execute(prepare, adapter_meta, query_meta, prepared, params, opts)
    raise "todo"
  end

  @impl Ecto.Adapter.Queryable
  def stream(_adapter_meta, _query_meta, _query, _params, _opts) do
    # Ecto.Adapters.SQL.stream(adapter_meta, query_meta, query, params, opts)
    raise "todo"
  end

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        header,
        rows,
        on_conflict,
        returning,
        placeholders,
        opts
      ) do
    Ecto.Adapters.SQLite.Schema.insert_all(
      adapter_meta,
      schema_meta,
      @conn,
      header,
      rows,
      on_conflict,
      returning,
      placeholders,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def insert(
        adapter_meta,
        %{source: source, prefix: prefix},
        params,
        {kind, conflict_params, _} = on_conflict,
        returning,
        opts
      ) do
    {fields, values} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning, [])

    Ecto.Adapters.SQLite.Schema.struct(
      adapter_meta,
      @conn,
      sql,
      :insert,
      source,
      [],
      values ++ conflict_params,
      kind,
      returning,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def update(adapter_meta, %{source: source, prefix: prefix}, fields, params, returning, opts) do
    {fields, field_values} = :lists.unzip(fields)
    filter_values = Keyword.values(params)
    sql = @conn.update(prefix, source, fields, params, returning)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :update,
      source,
      params,
      field_values ++ filter_values,
      :raise,
      returning,
      opts
    )
  end

  @impl Ecto.Adapter.Schema
  def delete(_adapter_meta, %{source: _source, prefix: _prefix}, _params, _opts) do
    # filter_values = Keyword.values(params)
    # sql = @conn.delete(prefix, source, params, [])

    # Ecto.Adapters.SQL.struct(
    #   adapter_meta,
    #   @conn,
    #   sql,
    #   :delete,
    #   source,
    #   params,
    #   filter_values,
    #   :raise,
    #   [],
    #   opts
    # )
    raise "todo"
  end

  @impl Ecto.Adapter.Transaction
  def transaction(_meta, _opts, _fun) do
    # Ecto.Adapters.SQL.transaction(meta, opts, fun)
    raise "todo"
  end

  @impl Ecto.Adapter.Transaction
  def in_transaction?(_meta) do
    # Ecto.Adapters.SQL.in_transaction?(meta)
    raise "todo"
  end

  @impl Ecto.Adapter.Transaction
  def rollback(_meta, _value) do
    # Ecto.Adapters.SQL.rollback(meta, value)
    raise "todo"
  end

  @impl Ecto.Adapter.Migration
  def execute_ddl(_meta, _definition, _opts) do
    # Ecto.Adapters.SQL.execute_ddl(meta, @conn, definition, opts)
    raise "todo"
  end

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, f), do: f.()

  @impl Ecto.Adapter.Storage
  defdelegate storage_up(opts), to: Ecto.Adapters.SQLite.Storage

  @impl Ecto.Adapter.Storage
  defdelegate storage_down(opts), to: Ecto.Adapters.SQLite.Storage

  @impl Ecto.Adapter.Storage
  defdelegate storage_status(opts), to: Ecto.Adapters.SQLite.Storage

  @impl Ecto.Adapter.Structure
  defdelegate structure_dump(default, config), to: Ecto.Adapters.SQLite.Structure

  @impl Ecto.Adapter.Structure
  defdelegate structure_load(default, config), to: Ecto.Adapters.SQLite.Structure

  @impl Ecto.Adapter.Structure
  def dump_cmd(_args, _opts, _config) do
    raise "not implemented"
  end
end
