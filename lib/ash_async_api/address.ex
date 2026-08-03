defmodule AshAsyncApi.Address do
  @moduledoc """
  Building, interpolating and matching channel addresses.

  An address is a **list of segments** joined by the delimiter of whichever bus carries the
  channel. The same declaration therefore produces the right shape on every broker:

      channel :ticket_events, ["helpdesk", "tickets", :id, "events"]

      # on MQTT   → helpdesk/tickets/{id}/events
      # on NATS   → helpdesk.tickets.{id}.events
      # on Redis  → helpdesk:tickets:{id}:events

  You do not write the delimiter, because it is not a property of your API — it is a
  property of the transport. See `AshAsyncApi.Server` for where the default comes from.

  ## Segments

  | Form | Meaning | Parameter name |
  | ---- | ------- | -------------- |
  | `"helpdesk"` | a literal | — |
  | `:id` | a field on the resource | `:id` |
  | `[:organization, :id]` | a relationship traversal | `:organization_id` |
  | `{:org, [:organization, :id]}` | a traversal with an explicit name | `:org` |

  Anything that is not a literal becomes a **parameter**: interpolated from the record when
  publishing, and extracted from the concrete address when receiving.

      channel :ticket_events, ["helpdesk", :organization_id, "tickets", :id, "events"]
      channel :ticket_events, ["helpdesk", [:organization, :slug], "tickets", :id]

  ## Special segments

  Four segments describe the *declaration site* rather than a record field, which is what
  lets one shared declaration — a `Spark.Dsl.Fragment`, say — produce a distinct, fully
  concrete channel on every resource it is applied to:

  | Form | Resolves to | When |
  | ---- | ----------- | ---- |
  | `:_domain` | the domain's `type` (its short name, snake-cased, by default) | building the routing table |
  | `:_resource` | the resource's `async_api.type` | building the routing table |
  | `:_pkey` | the record's primary key — the key field itself when there is one, a `{pkey}` parameter joining the fields with `-` when composite | table build / publish |
  | `:_event` | the operation's event verb — `created`/`updated`/`destroyed` by action type, overridable with `event_name` | publish |

      channel :events, [:_domain, :_resource, :_event, :_pkey]

      # on the Ticket resource of a domain typed "helpdesk", carried by NATS:
      #   helpdesk.ticket.{event}.{id}
      # and a created ticket publishes to
      #   helpdesk.ticket.created.9f2c...

  `:_domain`, `:_resource` and `:_pkey` are resolved against the declaring scope by
  `AshAsyncApi.Router.Table`; compiled standalone (before that resolution) they appear as
  `{domain}`, `{resource}` and `{pkey}` parameters.

  ## String addresses

  A plain string still works, for addresses a segment list cannot express — a parameter that
  is only part of a segment, say:

      channel :ticket_events, "helpdesk/tickets/id-{ticket_id}/events"

  In string form the delimiter is auto-detected (`/` if present, otherwise `.`) unless one is
  supplied, and `{braces}` mark the parameters.
  """

  defstruct [:raw, :parts, :params, :param_paths, :delimiter, :regex, :template]

  @typedoc """
  Where a parameter's value comes from.

    * `[atom()]` — a field read or relationship walk on the record.
    * `{:join, [atom()], String.t()}` — several fields, joined — how a composite
      primary key becomes one address token.
    * `{:context, atom()}` — supplied by the publisher rather than the record, e.g the
      operation's event verb.
    * `{:special, atom()}` — an unresolved special segment; `AshAsyncApi.Router.Table`
      rewrites these against the declaring scope.
    * `nil` — unknown; the value must arrive in the params map.
  """
  @type param_path ::
          [atom()] | {:join, [atom()], String.t()} | {:context, atom()} | {:special, atom()} | nil

  @typedoc "One piece of a compiled address."
  @type part :: {:literal, String.t()} | {:param, atom(), param_path()}

  @typedoc """
  A segment as written in the DSL: a literal, a field, a relationship path, a
  `{name, path}` pair, or one of the special segments (`:_domain`, `:_resource`,
  `:_event`, `:_pkey`).
  """
  @type segment :: String.t() | atom() | [atom()] | {atom(), atom() | [atom()] | tuple()}

  @special_segments %{
    _domain: :domain,
    _resource: :resource,
    _event: :event,
    _pkey: :pkey
  }

  @type t :: %__MODULE__{
          raw: [segment()] | String.t(),
          parts: [part()],
          params: [atom()],
          param_paths: %{atom() => [atom()] | nil},
          delimiter: String.t() | nil,
          regex: Regex.t(),
          template: String.t()
        }

  @placeholder ~r/\{([^{}]+)\}/

  @default_delimiter "/"

  @doc """
  Compile a segment list (or a string) into a `t:t/0`.

  ## Options

    * `:delimiter` — what joins the segments. Defaults to `"/"` for a segment list; for a
      string address it is auto-detected from the address itself.

  ## Examples

      iex> compiled = AshAsyncApi.Address.compile(["helpdesk", "tickets", :id, "events"], delimiter: ".")
      iex> compiled.template
      "helpdesk.tickets.{id}.events"
      iex> compiled.params
      [:id]

      iex> compiled = AshAsyncApi.Address.compile(["tickets", [:organization, :id]])
      iex> compiled.template
      "tickets/{organization_id}"
      iex> compiled.param_paths
      %{organization_id: [:organization, :id]}

      iex> AshAsyncApi.Address.compile("helpdesk/tickets/{ticket_id}").params
      [:ticket_id]
  """
  @spec compile([segment()] | String.t(), keyword()) :: t()
  def compile(raw, opts \\ [])

  def compile(segments, opts) when is_list(segments) do
    delimiter = opts[:delimiter] || @default_delimiter

    segments
    |> Enum.map(&to_part/1)
    |> Enum.intersperse({:literal, delimiter})
    |> merge_literals()
    |> build(segments, delimiter)
  end

  def compile(address, opts) when is_binary(address) do
    parts = parse(address)
    delimiter = opts[:delimiter] || detect_delimiter(parts)

    build(parts, address, delimiter)
  end

  defp build(parts, raw, delimiter) do
    params = for {:param, name, _path} <- parts, do: name

    %__MODULE__{
      raw: raw,
      parts: parts,
      params: params,
      param_paths: Map.new(for {:param, name, path} <- parts, do: {name, path}),
      delimiter: delimiter,
      regex: build_regex(parts, delimiter),
      template: render_template(parts)
    }
  end

  # ── Segments ─────────────────────────────────────────────────────────────────────────

  defp to_part(literal) when is_binary(literal), do: {:literal, literal}

  defp to_part(special) when is_map_key(@special_segments, special) do
    name = Map.fetch!(@special_segments, special)
    {:param, name, {:special, name}}
  end

  defp to_part(field) when is_atom(field) and not is_nil(field), do: {:param, field, [field]}

  defp to_part(path) when is_list(path) do
    if path != [] and Enum.all?(path, &is_atom/1) do
      {:param, path_name(path), path}
    else
      raise ArgumentError, """
      Invalid address segment #{inspect(path)}.

      A list segment is a relationship path and must contain only atoms, e.g
      #{inspect([:organization, :id])}.
      """
    end
  end

  defp to_part({name, {:join, fields, joiner}}) when is_atom(name) and is_binary(joiner) do
    {:param, name, {:join, validate_path!(fields, name), joiner}}
  end

  defp to_part({name, {:context, key}}) when is_atom(name) and is_atom(key) do
    {:param, name, {:context, key}}
  end

  defp to_part({name, path}) when is_atom(name) do
    {:param, name, path |> List.wrap() |> validate_path!(name)}
  end

  defp to_part(other) do
    raise ArgumentError, """
    Invalid address segment #{inspect(other)}.

    A segment is one of:

      "a literal"                    a literal string
      :field                         a field on the resource
      [:relationship, :field]        a relationship traversal
      {:name, [:relationship, :field]}   a traversal with an explicit parameter name
    """
  end

  defp validate_path!(path, name) do
    if path != [] and Enum.all?(path, &is_atom/1) do
      path
    else
      raise ArgumentError,
            "Invalid source path #{inspect(path)} for address parameter #{inspect(name)}; " <>
              "expected a field name or a list of atoms."
    end
  end

  # `[:organization, :id]` reads best as `organization_id` — the name a belongs_to would
  # already have given the foreign key.
  defp path_name(path), do: path |> Enum.map_join("_", &Atom.to_string/1) |> String.to_atom()

  defp merge_literals(parts) do
    parts
    |> Enum.reduce([], fn
      {:literal, right}, [{:literal, left} | rest] -> [{:literal, left <> right} | rest]
      part, acc -> [part | acc]
    end)
    |> Enum.reverse()
  end

  # ── Strings ──────────────────────────────────────────────────────────────────────────

  @doc """
  Split a string address into literal and parameter parts.

  ## Examples

      iex> AshAsyncApi.Address.parse("a/{b}/c")
      [{:literal, "a/"}, {:param, :b, [:b]}, {:literal, "/c"}]
  """
  @spec parse(String.t()) :: [part()]
  def parse(address) when is_binary(address) do
    @placeholder
    |> Regex.split(address, include_captures: true, trim: true)
    |> Enum.map(fn piece ->
      case Regex.run(@placeholder, piece) do
        [^piece, name] ->
          name = name |> String.trim() |> String.to_atom()
          {:param, name, [name]}

        _ ->
          {:literal, piece}
      end
    end)
  end

  # ── Introspection ────────────────────────────────────────────────────────────────────

  @doc "The parameter names in an address, in order."
  @spec params(t() | [segment()] | String.t()) :: [atom()]
  def params(%__MODULE__{params: params}), do: params
  def params(raw), do: raw |> compile() |> Map.fetch!(:params)

  @doc "The source path for each parameter."
  @spec param_paths(t()) :: %{atom() => [atom()] | nil}
  def param_paths(%__MODULE__{param_paths: paths}), do: paths

  @doc "Whether an address has any parameters."
  @spec templated?(t() | [segment()] | String.t()) :: boolean()
  def templated?(address), do: params(address) != []

  @doc """
  The `{braced}` template for an address, as it appears in the AsyncAPI document.

  ## Examples

      iex> AshAsyncApi.Address.template(["tickets", :id, "events"], delimiter: ".")
      "tickets.{id}.events"
  """
  @spec template(t() | [segment()] | String.t(), keyword()) :: String.t()
  def template(address, opts \\ [])
  def template(%__MODULE__{template: template}, _opts), do: template
  def template(raw, opts), do: raw |> compile(opts) |> Map.fetch!(:template)

  defp render_template(parts) do
    Enum.map_join(parts, fn
      {:literal, literal} -> literal
      {:param, name, _path} -> "{#{name}}"
    end)
  end

  # ── Interpolation ────────────────────────────────────────────────────────────────────

  @doc """
  Fill in an address from a map of parameter values.

  Returns `{:error, {:missing_params, names}}` when a parameter has no value.

  ## Examples

      iex> AshAsyncApi.Address.interpolate(["tickets", :id, "events"], %{id: 42})
      {:ok, "tickets/42/events"}

      iex> AshAsyncApi.Address.interpolate(["tickets", :id], %{})
      {:error, {:missing_params, [:id]}}
  """
  @spec interpolate(t() | [segment()] | String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def interpolate(%__MODULE__{parts: parts, delimiter: delimiter}, values),
    do: do_interpolate(parts, values, delimiter)

  def interpolate(raw, values), do: raw |> compile() |> interpolate(values)

  defp do_interpolate(parts, values, delimiter) do
    {pieces, missing} =
      Enum.reduce(parts, {[], []}, fn
        {:literal, literal}, {pieces, missing} ->
          {[literal | pieces], missing}

        {:param, name, _path}, {pieces, missing} ->
          case fetch_param(values, name) do
            {:ok, value} -> {[sanitize(to_address_value(value), delimiter) | pieces], missing}
            :error -> {pieces, [name | missing]}
          end
      end)

    case missing do
      [] -> {:ok, pieces |> Enum.reverse() |> IO.iodata_to_binary()}
      missing -> {:error, {:missing_params, Enum.reverse(missing)}}
    end
  end

  # A parameter value containing the delimiter would change the address's depth, and one
  # containing a wildcard could turn a concrete subject into a filter on some brokers.
  # Both are data leaking into structure, so they are flattened to `_` — the same rule
  # whatever the bus, to keep one record's address identical across transports.
  @unsafe_in_values ["*", ">", "#", "+", " ", "\t", "\n", "\r"]

  @doc """
  Make a value safe to embed as one address token.

  Replaces the delimiter, whitespace, and broker wildcard characters with `_`.

  ## Examples

      iex> AshAsyncApi.Address.sanitize("v1.2.3", ".")
      "v1_2_3"

      iex> AshAsyncApi.Address.sanitize("plain", ".")
      "plain"
  """
  @spec sanitize(String.t(), String.t() | nil) :: String.t()
  def sanitize(value, delimiter) do
    unsafe = if delimiter, do: [delimiter | @unsafe_in_values], else: @unsafe_in_values

    if String.contains?(value, unsafe) do
      String.replace(value, unsafe, "_")
    else
      value
    end
  end

  @doc "Like `interpolate/2`, but raises on missing parameters."
  @spec interpolate!(t() | [segment()] | String.t(), map()) :: String.t()
  def interpolate!(address, values) do
    case interpolate(address, values) do
      {:ok, interpolated} ->
        interpolated

      {:error, {:missing_params, missing}} ->
        raise ArgumentError,
              "cannot build address #{inspect(template(address))}, missing #{inspect(missing)}"
    end
  end

  # ── Matching ─────────────────────────────────────────────────────────────────────────

  @doc """
  Match a concrete address, extracting parameter values as strings.

  ## Examples

      iex> AshAsyncApi.Address.match(["tickets", :id, "events"], "tickets/42/events")
      {:ok, %{id: "42"}}

      iex> AshAsyncApi.Address.match(["tickets", :id], "tickets/42/nested")
      :error
  """
  @spec match(t() | [segment()] | String.t(), String.t()) ::
          {:ok, %{atom() => String.t()}} | :error
  def match(%__MODULE__{regex: regex, params: params}, concrete) when is_binary(concrete) do
    case Regex.run(regex, concrete, capture: :all_but_first) do
      nil -> :error
      captures -> {:ok, params |> Enum.zip(captures) |> Map.new()}
    end
  end

  def match(raw, concrete), do: raw |> compile() |> match(concrete)

  # ── Broker filters ───────────────────────────────────────────────────────────────────

  @doc """
  Translate an address into a broker subscription filter.

    * `{:single, wildcard}` — each parameter becomes `wildcard`, e.g `+` for MQTT.
    * `:multi_level` — truncate at the first parameter and append a multi-level wildcard.
    * `:exact` — strip the parameters, leaving the literal prefix. For brokers with no
      wildcards at all.

  ## Examples

      iex> AshAsyncApi.Address.to_filter(["tickets", :id, "events"], {:single, "+"})
      "tickets/+/events"

      iex> AshAsyncApi.Address.to_filter(AshAsyncApi.Address.compile(["tickets", :id, "events"], delimiter: "."), {:single, "*"})
      "tickets.*.events"

      iex> AshAsyncApi.Address.to_filter(["tickets", :id, "events"], :exact)
      "tickets"
  """
  @spec to_filter(t() | [segment()] | String.t(), {:single, String.t()} | :multi_level | :exact) ::
          String.t()
  def to_filter(address, style), do: address |> as_compiled() |> do_to_filter(style)

  defp do_to_filter(%__MODULE__{parts: parts}, {:single, wildcard}) do
    Enum.map_join(parts, fn
      {:literal, literal} -> literal
      {:param, _name, _path} -> wildcard
    end)
  end

  defp do_to_filter(%__MODULE__{parts: parts, delimiter: delimiter}, :multi_level) do
    multi = if delimiter == ".", do: ">", else: "#"

    case literal_prefix(parts) do
      "" -> multi
      prefix -> prefix <> multi
    end
  end

  defp do_to_filter(%__MODULE__{parts: parts, delimiter: delimiter}, :exact) do
    case literal_prefix(parts) do
      "" -> ""
      prefix when is_binary(delimiter) -> String.trim_trailing(prefix, delimiter)
      prefix -> prefix
    end
  end

  @doc """
  The literal prefix of an address, up to the first parameter.
  """
  @spec prefix(t() | [segment()] | String.t()) :: String.t()
  def prefix(address), do: address |> as_compiled() |> do_to_filter(:exact)

  defp literal_prefix(parts) do
    parts
    |> Enum.take_while(&match?({:literal, _}, &1))
    |> Enum.map_join(fn {:literal, literal} -> literal end)
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────────────

  defp as_compiled(%__MODULE__{} = compiled), do: compiled
  defp as_compiled(raw), do: compile(raw)

  defp fetch_param(values, name) when is_map(values) do
    with :error <- non_nil_fetch(values, name) do
      non_nil_fetch(values, Atom.to_string(name))
    end
  end

  defp non_nil_fetch(values, key) do
    case Map.fetch(values, key) do
      {:ok, nil} -> :error
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  defp to_address_value(value) when is_binary(value), do: value
  defp to_address_value(value) when is_atom(value), do: Atom.to_string(value)
  defp to_address_value(value) when is_integer(value), do: Integer.to_string(value)
  defp to_address_value(%Decimal{} = value), do: Decimal.to_string(value)

  defp to_address_value(value) do
    case Ash.Type.dump_to_embedded(Ash.Type.String, value, []) do
      {:ok, dumped} when is_binary(dumped) -> dumped
      _ -> to_string(value)
    end
  end

  defp detect_delimiter(parts) do
    joined =
      Enum.map_join(parts, fn
        {:literal, literal} -> literal
        {:param, _, _} -> ""
      end)

    cond do
      String.contains?(joined, "/") -> "/"
      String.contains?(joined, ".") -> "."
      true -> nil
    end
  end

  defp build_regex(parts, delimiter) do
    pattern =
      Enum.map_join(parts, fn
        {:literal, literal} -> Regex.escape(literal)
        {:param, _name, _path} -> param_pattern(delimiter)
      end)

    Regex.compile!("\\A" <> pattern <> "\\z")
  end

  defp param_pattern(nil), do: "(.+)"
  defp param_pattern(delimiter), do: "([^" <> Regex.escape(delimiter) <> "]+)"
end
