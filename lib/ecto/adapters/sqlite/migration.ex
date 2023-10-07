defmodule Ecto.Adapters.SQLite.Migration do
  @moduledoc false
  alias Ecto.Migration.{Table, Reference, Index, Constraint}
  @dialyzer :no_improper_lists

  defguardp is_create(command) when command in [:create, :create_if_not_exists]
  defguardp is_drop(command) when command in [:drop, :drop_if_exists]

  defp create(:create, %Table{}), do: "CREATE TABLE "
  defp create(:create_if_not_exists, %Table{}), do: "CREATE TABLE IF NOT EXISTS "
  defp create(:create, %Index{}), do: "CREATE INDEX "
  defp create(:create_if_not_exists, %Index{}), do: "CREATE INDEX IF NOT EXISTS "
  defp drop(:drop, %Table{}), do: "DROP TABLE "
  defp drop(:drop_if_exists, %Table{}), do: "DROP TABLE IF EXISTS "
  defp drop(:drop, %Index{}), do: "DROP INDEX "
  defp drop(:drop_if_exists, %Index{}), do: "DROP INDEX IF EXISTS "

  @spec execute_ddl(Ecto.Adapter.Migration.command()) :: iodata
  def execute_ddl({command, %Table{} = table, columns}) when is_create(command) do
    [
      [
        create(command, table),
        backtick(table.name),
        " (",
        columns(columns),
        pk(columns),
        ") " | table_options(table)
      ]
    ]
  end

  def execute_ddl({command, %Table{} = table, _mode}) when is_drop(command) do
    [
      [drop(command, table) | backtick(table.name)]
    ]
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    Enum.map(changes, fn change ->
      ["ALTER TABLE ", backtick(table.name), ?\s | column_change(change)]
    end)
  end

  def execute_ddl({command, %Index{} = index}) when is_create(command) do
    if index.concurrently do
      raise ArgumentError, "SQLite does not support CREATE INDEX CONCURRENTLY"
    end

    [
      [
        create(command, index),
        backtick(index.name),
        " ON ",
        backtick(index.table),
        " (",
        index_expr(index),
        ?) | index_where(index)
      ]
    ]
  end

  def execute_ddl({command, %Index{} = index, _mode}) when is_drop(command) do
    if index.concurrently do
      raise ArgumentError, "SQLite does not support DROP INDEX CONCURRENTLY"
    end

    [
      [drop(command, index) | backtick(index.name)]
    ]
  end

  def execute_ddl({_command, %Constraint{}}) do
    raise "SQLite does not support constraints"
  end

  def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [
      [
        "ALTER TABLE ",
        backtick(current_table.name),
        " RENAME TO " | backtick(new_table.name)
      ]
    ]
  end

  def execute_ddl({:rename, %Table{} = table, column_name, new_column_name}) do
    [
      [
        "ALTER TABLE ",
        backtick(table.name),
        " RENAME COLUMN ",
        backtick(column_name),
        " TO " | backtick(new_column_name)
      ]
    ]
  end

  def execute_ddl(string) when is_binary(string) do
    [string]
  end

  def execute_ddl(list) when is_list(list) do
    raise ArgumentError, "SQLite adapter does not support lists in execute_ddl"
  end

  defp backtick(name) do
    [?`, String.replace(name, "`", "``"), ?`]
  end

  defp columns(columns) do
    columns
    |> Enum.map(&column_definition/1)
    |> Enum.intersperse(?,)
  end

  defp pk(columns) do
    pk_columns =
      Enum.filter(columns, fn {_, _, _, opts} ->
        case Keyword.get(opts, :primary_key, false) do
          true = t -> t
          false = f -> f
        end
      end)

    case pk_columns do
      [] = empty ->
        empty

      pk_columns ->
        pk_expr =
          pk_columns
          |> Enum.map(fn {_, name, _, _} -> backtick(name) end)
          |> Enum.intersperse(?,)

        [",PRIMARY KEY (", pk_expr, ?)]
    end
  end

  # TODO
  defp column_definition({:add, _name, %Reference{}, _opts}) do
    raise ArgumentError, "SQLite does not support FOREIGN KEY"
  end

  defp column_definition({:add, name, type, opts}) do
    [backtick(name), ?\s, column_type(type) | column_options(type, opts)]
  end

  defp column_options(type, opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)
    [default_expr(default, type), null_expr(null)]
  end

  # TODO
  defp column_change({:add, _name, %Reference{}, _opts}) do
    raise ArgumentError, "SQLite does not support FOREIGN KEY"
  end

  defp column_change({:add, name, type, opts}) do
    [
      "ADD COLUMN ",
      backtick(name),
      ?\s,
      column_type(type)
      | column_options(type, opts)
    ]
  end

  defp column_change({:modify, _name, %Reference{}, _opts}) do
    raise ArgumentError, "SQLite does not support FOREIGN KEY"
  end

  defp column_change({:modify, _name, _type, _opts}) do
    [
      ["CREATE COLUMN "],
      [],
      [],
      []
      # create _rand_new column
      # insert from current to new
      # drop current
      # rename new

      # "MODIFY COLUMN ",
      # backtick(name),
      # ?\s,
      # column_type(type),
      # modify_default(name, type, opts)
      # | modify_null(name, opts)
    ]
  end

  defp column_change({:remove, name}) do
    ["DROP COLUMN " | backtick(name)]
  end

  defp column_change({:remove, name, _type, _opts}) do
    column_change({:remove, name})
  end

  # defp modify_null(_name, opts) do
  #   case Keyword.get(opts, :null) do
  #     nil -> []
  #     val -> null_expr(val)
  #   end
  # end

  defp null_expr(true), do: " NULL"
  defp null_expr(false), do: " NOT NULL"
  defp null_expr(_), do: []

  # defp modify_default(name, type, opts) do
  #   case Keyword.fetch(opts, :default) do
  #     {:ok, _val} = ok ->
  #       [" ADD ", default_expr(ok, type), " FOR ", backtick(name)]

  #     :error ->
  #       []
  #   end
  # end

  defp default_expr({:ok, nil}, _type) do
    " DEFAULT NULL"
  end

  defp default_expr({:ok, literal}, _type) when is_binary(literal) do
    [" DEFAULT " | backtick(literal)]
  end

  defp default_expr({:ok, literal}, _type) when is_number(literal) do
    [" DEFAULT " | to_string(literal)]
  end

  defp default_expr({:ok, {:fragment, expr}}, _type) do
    [" DEFAULT " | expr]
  end

  defp default_expr({:ok, true}, _type) do
    " DEFAULT 1"
  end

  defp default_expr({:ok, false}, _type) do
    " DEFAULT 0"
  end

  defp default_expr({:ok, list}, _type) when is_list(list) do
    raise ArgumentError,
          "SQLite adapter does not support lists in :default, " <>
            "use fragments instead"
  end

  defp default_expr({:ok, map}, _type) when is_map(map) do
    raise ArgumentError,
          "SQLite adapter does not support maps in :default, " <>
            "use fragments instead"
  end

  defp default_expr(:error, _), do: []

  defp index_expr(%Index{} = index) do
    index.columns |> Enum.map(&index_expr/1) |> Enum.intersperse(?,)
  end

  defp index_expr(literal) when is_binary(literal), do: literal
  defp index_expr(literal), do: backtick(literal)

  defp index_where(%Index{where: nil}), do: []

  defp index_where(%Index{where: where}) do
    raise RuntimeError, "TODO index.where = #{inspect(where)}"
  end

  @types_blob [:uuid, :binary, :binary_id]

  @types_integer [
    :boolean,
    :id,
    :serial,
    :bigserial,
    :integer,
    :bigint,
    :time,
    :utc_datetime,
    :utc_datetime_usec,
    :naive_datetime,
    :naive_datetime_usec
  ]

  defp column_type(:strint), do: "TEXT"
  defp column_type(:float), do: "REAL"
  defp column_type(t) when t in @types_blob, do: "BLOB"
  defp column_type(t) when t in @types_integer, do: "INTEGER"

  defp column_type(t) do
    raise ArgumentError, "type #{inspect(t)} is not supported"
  end

  defp table_options(%Table{options: options}) when is_binary(options) do
    options
  end

  defp table_options(%Table{options: nil}), do: []

  defp table_options(%Table{options: options}) do
    raise ArgumentError, "table options #{inspect(options)} are not supported"
  end
end
