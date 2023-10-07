defmodule Ecto.Adapters.SQLite.Connection do
  @moduledoc false
  @behaviour Ecto.Adapters.SQL.Connection
  @dialyzer :no_improper_lists

  alias Ecto.SubQuery
  alias Ecto.Query.{QueryExpr, BooleanExpr, Tagged}

  @parent_as __MODULE__

  @impl true
  def child_spec(_opts) do
    # TODO
    :ignore
  end

  @impl true
  def prepare_execute(db, _name, sql, args, _opts) do
    with {:ok, stmt} <- SQLite.prepare(db, sql) do
      case SQLite.bind(db, stmt, args) do
        :ok ->
          {:ok, rows} = SQLite.fetch_all(db, stmt, 100)
          {:ok, stmt, rows}

        {:error, _reason} = error ->
          :ok = SQLite.finalize(stmt)
          error
      end
    end
  end

  @impl true
  def execute(db, stmt, args, _opts) do
    with :ok <- SQLite.bind(db, stmt, args) do
      SQLite.fetch_all(db, stmt, 100)
    end
  end

  @impl true
  def query(db, sql, args, _opts) do
    SQLite.fetch_all(db, sql, args, 100)
  end

  @impl true
  def query_many(_db, _sql, _args, _opts) do
    raise "not implemented"
  end

  @impl true
  def stream(_db, _sql, _args, _opts) do
    raise "not implemented"
  end

  @impl true
  def to_constraints(_exception, _opts) do
    raise "not implemented"
  end

  @impl true
  def all(query, as_prefix \\ []) do
    if Map.get(query, :lock) do
      raise ArgumentError, "SQLite does not support locks"
    end

    sources = create_names(query, as_prefix)

    [
      cte(query, sources),
      select(query, sources),
      from(query, sources),
      join(query, sources),
      where(query, sources),
      group_by(query, sources),
      having(query, sources),
      window(query, sources),
      order_by(query, sources),
      limit(query, sources),
      offset(query, sources),
      combinations(query)
    ]
  end

  @impl true
  def update_all(query, _prefix \\ nil) do
    raise "TODO"
  end

  @impl true
  def delete_all(query) do
    # TODO
    unless query.joins == [] do
      raise Ecto.QueryError,
        query: query,
        message: "SQLite does not support JOIN on DELETE statements"
    end

    # TODO
    if query.select do
      raise Ecto.QueryError,
        query: query,
        message: "SQLite does not support RETURNING on DELETE statements"
    end

    # TODO
    if query.with_ctes do
      raise Ecto.QueryError,
        query: query,
        message: "SQLite does not support CTEs (WITH) on DELETE statements"
    end

    %{sources: sources} = query
    {table, _schema, _prefix} = elem(sources, 0)

    where =
      case query.wheres do
        [] = empty -> empty
        _ -> where(query, {{nil, nil, nil}})
      end

    ["DELETE FROM ", backtick(table) | where]
  end

  @impl true
  def ddl_logs(_), do: []

  @impl true
  def table_exists_query(table) do
    {"SELECT name FROM sqlite_master WHERE type=table AND name=? LIMIT 1", [table]}
  end

  @impl true
  def execute_ddl(command) do
    Ecto.Adapters.SQLite.Migration.execute_ddl(command)
  end

  @impl true
  def insert(prefix, table, header, rows, _on_conflict, returning, _placeholders) do
    # TODO
    unless returning == [] do
      raise ArgumentError, "SQLite does not support RETURNING on INSERT statements"
    end

    # TODO
    if prefix do
      raise ArgumentError, "SQLite does not support prefixes"
    end

    insert(table, header, rows)
  end

  def insert(table, header, rows) do
    insert = ["INSERT INTO ", backtick(table), ?(, intersperse_map(header, ?,, &backtick/1), ?)]

    case rows do
      {%Ecto.Query{} = query, params} -> [insert, ?\s | all(query, params)]
      rows when is_list(rows) -> insert
    end
  end

  @impl true
  def update(_prefix, _table, _fields, _filters, _returning) do
    raise ArgumentError, "TODO"
  end

  @impl true
  def delete(prefix, table, filters, returning) do
    # TODO
    if prefix do
      raise ArgumentError, "SQLite does not support prefixes"
    end

    unless returning == [] do
      raise ArgumentError, "SQLite does not support RETURNING on DELETE statements"
    end

    filters =
      intersperse_map(filters, " AND ", fn {field, value} ->
        expr =
          case value do
            nil -> " IS NULL"
            _ -> "=?"
          end

        [backtick(field) | expr]
      end)

    ["DELETE FROM ", backtick(table), " WHERE " | filters]
  end

  @impl true
  def explain_query(_db, _query, _args, _opts) do
    raise "TODO"
  end

  binary_ops = [
    ==: "=",
    !=: "!=",
    <=: "<=",
    >=: ">=",
    <: "<",
    >: ">",
    +: "+",
    -: "-",
    *: "*",
    /: "/",
    and: " AND ",
    or: " OR ",
    ilike: " ILIKE ",
    like: " LIKE ",
    # TODO these two are not in binary_ops in sqlite3 adapter
    in: " IN ",
    is_nil: " WHERE "
  ]

  @binary_ops Keyword.keys(binary_ops)

  for {op, str} <- binary_ops do
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end

  defp handle_call(fun, _arity), do: {:fun, String.Chars.Atom.to_string(fun)}

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
    [
      "SELECT ",
      distinct(distinct, sources, query)
      | select_fields(fields, sources, query)
    ]
  end

  defp select_fields([], _sources, _query), do: "1"

  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ?,, fn
      # TODO
      {:&, _, [idx]} ->
        {_, source, _} = elem(sources, idx)
        source

      {k, v} ->
        [expr(v, sources, query), " AS " | backtick(k)]

      v ->
        expr(v, sources, query)
    end)
  end

  defp distinct(nil, _sources, _query), do: []
  defp distinct(%{expr: true}, _sources, _query), do: "DISTINCT "
  defp distinct(%{expr: false}, _sources, _query), do: []

  defp distinct(%{expr: exprs}, _sources, query) when is_list(exprs) do
    raise Ecto.QueryError, message: "SQLite doesn't support DISTINCT ON", query: query
  end

  defp from(%{from: %{source: source, hints: hints}} = query, sources) do
    # TODO
    unless hints == [] do
      raise Ecto.QueryError, message: "SQLite doesn't support hints"
    end

    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, " AS " | name]
  end

  defp cte(%{with_ctes: %{recursive: recursive, queries: [_ | _] = queries}} = query, sources) do
    ctes =
      intersperse_map(queries, ?,, fn {name, _opts, cte} ->
        [backtick(name), " AS " | cte_query(cte, sources, query)]
      end)

    case recursive do
      true -> ["WITH RECURSIVE ", ctes, ?\s]
      false -> ["WITH ", ctes, ?\s]
    end
  end

  defp cte(_query, _sources), do: []

  defp cte_query(%Ecto.Query{} = query, sources, parent_query) do
    query = put_in(query.aliases[@parent_as], {parent_query, sources})
    [?(, all(query, subquery_as_prefix(sources)), ?)]
  end

  defp cte_query(%QueryExpr{expr: expr}, sources, query) do
    expr(expr, sources, query)
  end

  defp join(%{joins: []}, _sources), do: []

  defp join(%{joins: joins} = query, sources) do
    Enum.map(joins, fn join ->
      %{qual: qual, ix: ix, source: source, on: %{expr: on_exrp}, hints: hints} =
        join

      unless hints == [] do
        raise Ecto.QueryError,
          query: query,
          message: "SQLite does not support hints on JOIN"
      end

      {join, name} = get_source(query, sources, ix, source)
      [join_qual(qual), join, " AS ", name | join_on(qual, on_exrp, sources, query)]
    end)
  end

  defp join_on(:cross, true, _sources, _query), do: []

  defp join_on(_qual, expr, sources, query) do
    [" ON " | expr(expr, sources, query)]
  end

  defp join_qual(:inner), do: " INNER JOIN "
  defp join_qual(:left), do: " LEFT OUTER JOIN "
  defp join_qual(:right), do: " RIGHT OUTER JOIN "
  defp join_qual(:full), do: " FULL OUTER JOIN "
  defp join_qual(:cross), do: " CROSS JOIN "

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%{group_bys: []}, _sources), do: []

  defp group_by(%{group_bys: group_bys} = query, sources) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ?,, fn %{expr: expr} ->
          intersperse_map(expr, ?,, &expr(&1, sources, query))
        end)
    ]
  end

  defp window(%{windows: []}, _sources), do: []

  defp window(%{windows: windows} = query, sources) do
    [
      " WINDOW "
      | intersperse_map(windows, ?,, fn {name, %{expr: kw}} ->
          [backtick(name), " AS " | window_exprs(kw, sources, query)]
        end)
    ]
  end

  defp window_exprs(kw, sources, query) do
    [?(, intersperse_map(kw, ?\s, &window_expr(&1, sources, query)), ?)]
  end

  defp window_expr({:partition_by, fields}, sources, query) do
    ["PARTITION BY " | intersperse_map(fields, ?,, &expr(&1, sources, query))]
  end

  defp window_expr({:order_by, fields}, sources, query) do
    ["ORDER BY " | intersperse_map(fields, ?,, &order_by_expr(&1, sources, query))]
  end

  defp window_expr({:frame, {:fragment, _, _} = fragment}, sources, query) do
    expr(fragment, sources, query)
  end

  defp order_by(%{order_bys: []}, _sources), do: []

  defp order_by(%{order_bys: order_bys} = query, sources) do
    [
      " ORDER BY "
      | intersperse_map(order_bys, ?,, fn %{expr: expr} ->
          intersperse_map(expr, ?,, &order_by_expr(&1, sources, query))
        end)
    ]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)

    case dir do
      :asc ->
        str

      :desc ->
        [str | " DESC"]

      :asc_nulls_first ->
        [str | " ASC NULLS FIRST"]

      :desc_nulls_first ->
        [str | " DESC NULLS FIRST"]

      :asc_nulls_last ->
        [str | " ASC NULLS LAST"]

      :desc_nulls_last ->
        [str | " DESC NULLS LAST"]

      _ ->
        raise Ecto.QueryError,
          query: query,
          message: "SQLite does not support #{dir} in ORDER BY"
    end
  end

  defp limit(%{limit: nil}, _sources), do: []

  defp limit(%{limit: %{expr: expr}} = query, sources) do
    [" LIMIT ", expr(expr, sources, query)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %{expr: expr}} = query, sources) do
    [" OFFSET ", expr(expr, sources, query)]
  end

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, &combination/1)
  end

  defp combination({:union, query}), do: [" UNION (", all(query), ?)]
  defp combination({:union_all, query}), do: [" UNION ALL (", all(query), ?)]
  defp combination({:except, query}), do: [" EXCEPT (", all(query), ?)]
  defp combination({:intersect, query}), do: [" INTERSECT (", all(query), ?)]

  defp combination({:except_all, query}) do
    raise Ecto.QueryError,
      query: query,
      message: "SQLite does not support EXCEPT ALL"
  end

  defp combination({:intersect_all, query}) do
    raise Ecto.QueryError,
      query: query,
      message: "SQLite does not support INTERSECT ALL"
  end

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | exprs], sources, query) do
    {_, result} =
      Enum.reduce(exprs, {op, paren_expr(expr, sources, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op) | paren_expr(expr, sources, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op) | paren_expr(expr, sources, query)]}
      end)

    [name | result]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  # TODO
  defp parens_for_select([first_expr | _] = expression) do
    if is_binary(first_expr) and String.match?(first_expr, ~r/^\s*select/i) do
      [?(, expression, ?)]
    else
      expression
    end
  end

  defp paren_expr(expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  defp expr({_type, [literal]}, sources, query), do: expr(literal, sources, query)
  defp expr({:^, [], [_ix]}, _sources, _query), do: ??

  defp expr({:^, [], [_, len]}, _sources, _query) when len > 0 do
    [?(, ?? |> List.duplicate(len) |> Enum.intersperse(?,), ?)]
  end

  defp expr({:^, [], [_, 0]}, _sources, _query), do: "()"

  defp expr({{:., _, [{:&, _, [ix]}, field]}, _, []}, sources, _query) when is_atom(field) do
    backtick_qualified_name(field, sources, ix)
  end

  defp expr({{:., _, [{:parent_as, _, [as]}, field]}, _, []}, _sources, query)
       when is_atom(field) do
    {ix, sources} = get_parent_sources_ix(query, as)
    backtick_qualified_name(field, sources, ix)
  end

  # TODO
  defp expr({:&, _, [ix]}, sources, _query) do
    {_, source, _} = elem(sources, ix)
    source
  end

  # TODO
  defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
    {_, name, schema} = elem(sources, idx)

    if is_nil(schema) and is_nil(fields) do
      raise Ecto.QueryError,
        query: query,
        message: """
        SQLite requires a schema module when using selector #{inspect(name)} but none was given. \
        Please specify a schema or specify exactly which fields from #{inspect(name)} you desire\
        """
    end

    intersperse_map(fields, ?,, &[name, ?. | backtick(&1)])
  end

  defp expr({:in, _, [_left, []]}, _sources, _query), do: "0"

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [_, {:^, _, [_ix, 0]}]}, _sources, _query), do: "0"

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " IN ", expr(right, sources, query)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr({:filter, _, [agg, filter]}, sources, query) do
    [expr(agg, sources, query), " FILTER (WHERE ", expr(filter, sources, query), ?)]
  end

  defp expr(%SubQuery{query: query}, sources, parent_query) do
    query = put_in(query.aliases[@parent_as], {parent_query, sources})
    [?(, all(query, subquery_as_prefix(sources)), ?)]
  end

  defp expr({:fragment, _, [kw]}, _sources, query)
       when is_list(kw) or tuple_size(kw) == 3 do
    raise Ecto.QueryError,
      query: query,
      message: "SQLite adapter does not support keyword or interpolated fragments"
  end

  defp expr({:fragment, _, parts}, sources, query) do
    parts
    |> Enum.map(fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
    |> parens_for_select()
  end

  defp expr({:literal, _, [literal]}, _sources, _query), do: backtick(literal)
  defp expr({:selected_as, _, [name]}, _sources, _query), do: backtick(name)

  defp expr({:over, _, [agg, name]}, sources, query) when is_atom(name) do
    [expr(agg, sources, query), " OVER " | backtick(name)]
  end

  defp expr({:over, _, [agg, kw]}, sources, query) do
    [expr(agg, sources, query), " OVER " | window_exprs(kw, sources, query)]
  end

  defp expr({:{}, _, elems}, sources, query) do
    [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"

  # TODO
  # defp expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
  #   [expr(datetime, sources, query), ?+, interval(count, interval, sources, query)]
  # end

  # defp expr({:date_add, _, [date, count, interval]}, sources, query) do
  #   [expr(date, sources, query), ?+, interval(count, interval, sources, query)]
  # end

  defp expr({:json_extract_path, _, [expr, path]}, sources, query) do
    path =
      Enum.map(path, fn
        bin when is_binary(bin) -> [?., escape_json_key(bin)]
        int when is_integer(int) -> [?[, String.Chars.Integer.to_string(int), ?]]
      end)

    ["json(", expr(expr, sources, query), ", '$", path | "')"]
  end

  # TODO parens?
  defp expr({:exists, _, [subquery]}, sources, query) do
    ["exists" | expr(subquery, sources, query)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ?,, &expr(&1, sources, query)), ?)]
    end
  end

  # TODO
  defp expr(list, sources, query) when is_list(list) do
    [?(, intersperse_map(list, ?,, &expr(&1, sources, query)), ?)]
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Tagged{value: value, type: :any}, sources, query) do
    expr(value, sources, query)
  end

  # TODO
  # defp expr(%Tagged{value: value, type: type}, sources, query) do
  #   ["CAST(", expr(value, sources, query), " AS ", ecto_to_db(type, query), ?)]
  # end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?', escape_string(literal), ?']
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    String.Chars.Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    String.Chars.Float.to_string(literal)
  end

  defp expr(expr, _sources, query) do
    raise Ecto.QueryError,
      query: query,
      message: "unsupported expression #{inspect(expr)}"
  end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary({:is_nil, _, [_]} = expr, sources, query) do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp create_names(%{sources: sources}, as_prefix) do
    sources |> create_names(0, tuple_size(sources), as_prefix) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit, as_prefix) when pos < limit do
    [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
  end

  defp create_names(_sources, pos, pos, as_prefix), do: [as_prefix]

  defp subquery_as_prefix(sources) do
    [?s | :erlang.element(tuple_size(sources), sources)]
  end

  defp create_name(sources, pos, as_prefix) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, as_prefix ++ [?f | String.Chars.Integer.to_string(pos)], nil}

      {table, schema, _prefix} ->
        name = as_prefix ++ [create_alias(table) | String.Chars.Integer.to_string(pos)]
        {backtick(table), name, schema}

      %SubQuery{} ->
        {nil, as_prefix ++ [?s | String.Chars.Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::bytes>>)
       when first in ?a..?z
       when first in ?A..?Z,
       do: first

  defp create_alias(_), do: ?t

  defp intersperse_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperse_map(rest, separator, mapper)]
  end

  defp intersperse_map([], _separator, _mapper), do: []

  defp backtick_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)

    case source do
      nil -> backtick(name)
      _other -> [source, ?. | backtick(name)]
    end
  end

  def backtick(value) when is_binary(value) do
    # TODO faster
    [?`, String.replace(value, "`", ""), ?`]
  end

  def backtick(value) when is_atom(value) do
    value |> String.Chars.Atom.to_string() |> backtick()
  end

  # TODO faster?
  defp escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("'", "''", [:global])
    |> :binary.replace("\\", "\\\\", [:global])
  end

  defp escape_json_key(value) when is_binary(value) do
    value
    |> escape_string()
    |> :binary.replace("\"", "\\\"", [:global])
  end

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || expr(source, sources, query), name}
  end

  defp get_parent_sources_ix(query, as) do
    case query.aliases[@parent_as] do
      {%{aliases: %{^as => ix}}, sources} -> {ix, sources}
      {%{} = parent, _sources} -> get_parent_sources_ix(parent, as)
    end
  end
end
