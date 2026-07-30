defmodule AshAsyncApi.Address do
  @moduledoc """
  Compiling, interpolating and matching templated channel addresses.

  A channel address is a string with zero or more `{parameter}` placeholders:

      "helpdesk/tickets/{ticket_id}/events"

  Three things need to happen with that template:

    * **Interpolation** — on the way out, fill the placeholders in from a record to
      get a concrete address to publish to.
    * **Matching** — on the way in, decide which channel a concrete address belongs
      to and extract the parameter values.
    * **Filtering** — translate the template into whatever wildcard syntax the
      broker subscribes with (`+` for MQTT, `*` for NATS, ...).

  ## Separators

  A placeholder matches a single address *segment*. The separator is detected from
  the template: `/` if present, otherwise `.` (NATS style), otherwise the
  placeholder matches greedily to the end. Pass `separator: "."` to force it.
  """

  defstruct [:address, :segments, :params, :separator, :regex]

  @type segment :: {:literal, String.t()} | {:param, atom()}

  @type t :: %__MODULE__{
          address: String.t(),
          segments: [segment()],
          params: [atom()],
          separator: String.t() | nil,
          regex: Regex.t()
        }

  @placeholder ~r/\{([^{}]+)\}/

  @doc """
  Compile a templated address into a `t:t/0`.

  ## Options

    * `:separator` — the address segment separator. Auto-detected when omitted.

  ## Examples

      iex> compiled = AshAsyncApi.Address.compile("helpdesk/tickets/{ticket_id}/events")
      iex> compiled.params
      [:ticket_id]
      iex> compiled.separator
      "/"

      iex> AshAsyncApi.Address.compile("helpdesk.tickets.{ticket_id}").separator
      "."
  """
  @spec compile(String.t(), keyword()) :: t()
  def compile(address, opts \\ []) when is_binary(address) do
    segments = parse(address)
    separator = opts[:separator] || detect_separator(segments)

    %__MODULE__{
      address: address,
      segments: segments,
      params: for({:param, name} <- segments, do: name),
      separator: separator,
      regex: build_regex(segments, separator)
    }
  end

  @doc """
  Split a templated address into literal and parameter segments.

  ## Examples

      iex> AshAsyncApi.Address.parse("a/{b}/c")
      [literal: "a/", param: :b, literal: "/c"]
  """
  @spec parse(String.t()) :: [segment()]
  def parse(address) when is_binary(address) do
    @placeholder
    |> Regex.split(address, include_captures: true, trim: true)
    |> Enum.map(fn part ->
      case Regex.run(@placeholder, part) do
        [^part, name] -> {:param, String.to_atom(String.trim(name))}
        _ -> {:literal, part}
      end
    end)
  end

  @doc """
  The parameter names in a templated address, in order.
  """
  @spec params(String.t() | t()) :: [atom()]
  def params(%__MODULE__{params: params}), do: params
  def params(address) when is_binary(address), do: for({:param, name} <- parse(address), do: name)

  @doc """
  Whether an address contains any parameters.
  """
  @spec templated?(String.t() | t()) :: boolean()
  def templated?(address), do: params(address) != []

  @doc """
  Fill in an address template from a map of values.

  Values may be keyed by atom or string. Returns `{:error, {:missing_params, names}}`
  if any placeholder has no value.

  ## Examples

      iex> AshAsyncApi.Address.interpolate("tickets/{id}/events", %{id: 42})
      {:ok, "tickets/42/events"}

      iex> AshAsyncApi.Address.interpolate("tickets/{id}", %{})
      {:error, {:missing_params, [:id]}}
  """
  @spec interpolate(String.t() | t(), map()) :: {:ok, String.t()} | {:error, term()}
  def interpolate(%__MODULE__{segments: segments}, values), do: do_interpolate(segments, values)

  def interpolate(address, values) when is_binary(address),
    do: do_interpolate(parse(address), values)

  defp do_interpolate(segments, values) do
    {parts, missing} =
      Enum.reduce(segments, {[], []}, fn
        {:literal, literal}, {parts, missing} ->
          {[literal | parts], missing}

        {:param, name}, {parts, missing} ->
          case fetch_param(values, name) do
            {:ok, value} -> {[to_address_value(value) | parts], missing}
            :error -> {parts, [name | missing]}
          end
      end)

    case missing do
      [] -> {:ok, parts |> Enum.reverse() |> IO.iodata_to_binary()}
      missing -> {:error, {:missing_params, Enum.reverse(missing)}}
    end
  end

  @doc """
  Same as `interpolate/2` but raises on missing parameters.
  """
  @spec interpolate!(String.t() | t(), map()) :: String.t()
  def interpolate!(address, values) do
    case interpolate(address, values) do
      {:ok, interpolated} ->
        interpolated

      {:error, {:missing_params, missing}} ->
        raise ArgumentError,
              "cannot build address #{inspect(address_string(address))}, " <>
                "missing values for #{inspect(missing)}"
    end
  end

  @doc """
  Match a concrete address against a template, extracting parameter values.

  Returns `{:ok, params}` with string values, or `:error` if it does not match.

  ## Examples

      iex> AshAsyncApi.Address.match("tickets/{id}/events", "tickets/42/events")
      {:ok, %{id: "42"}}

      iex> AshAsyncApi.Address.match("tickets/{id}/events", "tickets/42/other")
      :error

      iex> AshAsyncApi.Address.match("tickets/{id}", "tickets/42/nested")
      :error
  """
  @spec match(String.t() | t(), String.t()) :: {:ok, %{atom() => String.t()}} | :error
  def match(%__MODULE__{regex: regex, params: params}, concrete) when is_binary(concrete) do
    case Regex.run(regex, concrete, capture: :all_but_first) do
      nil -> :error
      captures -> {:ok, params |> Enum.zip(captures) |> Map.new()}
    end
  end

  def match(address, concrete) when is_binary(address),
    do: match(compile(address), concrete)

  @doc """
  Translate a template into a broker subscription filter.

  `style` describes the wildcard syntax the broker uses:

    * `{:single, wildcard}` — each parameter becomes `wildcard`, e.g `+` for MQTT
      or `*` for NATS.
    * `:multi_level` — the template is truncated at the first parameter and a
      multi-level wildcard is appended. Only useful when the broker cannot match a
      single level.
    * `:exact` — parameters are stripped, yielding the literal prefix. For brokers
      with no wildcards at all (Kafka), where you subscribe to a whole topic.

  ## Examples

      iex> AshAsyncApi.Address.to_filter("tickets/{id}/events", {:single, "+"})
      "tickets/+/events"

      iex> AshAsyncApi.Address.to_filter("tickets.{id}.events", {:single, "*"})
      "tickets.*.events"

      iex> AshAsyncApi.Address.to_filter("tickets/{id}/events", :multi_level)
      "tickets/#"
  """
  @spec to_filter(String.t() | t(), {:single, String.t()} | :multi_level | :exact) :: String.t()
  def to_filter(address, style) do
    compiled = as_compiled(address)
    do_to_filter(compiled, style)
  end

  defp do_to_filter(%__MODULE__{segments: segments}, {:single, wildcard}) do
    segments
    |> Enum.map_join(fn
      {:literal, literal} -> literal
      {:param, _} -> wildcard
    end)
  end

  defp do_to_filter(%__MODULE__{segments: segments, separator: separator}, :multi_level) do
    multi = if separator == ".", do: ">", else: "#"

    segments
    |> Enum.take_while(&match?({:literal, _}, &1))
    |> Enum.map_join(fn {:literal, literal} -> literal end)
    |> case do
      "" -> multi
      prefix -> prefix <> multi
    end
  end

  defp do_to_filter(%__MODULE__{segments: segments, separator: separator}, :exact) do
    segments
    |> Enum.take_while(&match?({:literal, _}, &1))
    |> Enum.map_join(fn {:literal, literal} -> literal end)
    |> then(fn
      "" -> ""
      prefix when is_binary(separator) -> String.trim_trailing(prefix, separator)
      prefix -> prefix
    end)
  end

  @doc """
  The static prefix of an address, up to the first parameter.

  Useful as a `Group` prefix key, since `Group.members/3` treats a trailing `/`
  as a prefix query.
  """
  @spec prefix(String.t() | t()) :: String.t()
  def prefix(address), do: do_to_filter(as_compiled(address), :exact)

  defp as_compiled(%__MODULE__{} = compiled), do: compiled
  defp as_compiled(address) when is_binary(address), do: compile(address)

  defp address_string(%__MODULE__{address: address}), do: address
  defp address_string(address) when is_binary(address), do: address

  defp fetch_param(values, name) when is_map(values) do
    case Map.fetch(values, name) do
      {:ok, nil} -> :error
      {:ok, value} -> {:ok, value}
      :error -> fetch_string_param(values, name)
    end
  end

  defp fetch_string_param(values, name) do
    case Map.fetch(values, Atom.to_string(name)) do
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

  defp detect_separator(segments) do
    literals = for {:literal, literal} <- segments, do: literal
    joined = Enum.join(literals)

    cond do
      String.contains?(joined, "/") -> "/"
      String.contains?(joined, ".") -> "."
      true -> nil
    end
  end

  defp build_regex(segments, separator) do
    pattern =
      segments
      |> Enum.map_join(fn
        {:literal, literal} -> Regex.escape(literal)
        {:param, _} -> param_pattern(separator)
      end)

    Regex.compile!("\\A" <> pattern <> "\\z")
  end

  defp param_pattern(nil), do: "(.+)"
  defp param_pattern(separator), do: "([^" <> Regex.escape(separator) <> "]+)"
end
