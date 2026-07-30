defmodule AshAsyncApi.Spec.Schema do
  @moduledoc """
  JSON Schema derivation from Ash types.

  The schemas here have to agree with what `AshAsyncApi.Payload` actually puts on the
  wire, since both are derived from the same field lists and the same Ash types. A
  document that describes a `format: date-time` where the runtime sends an integer is
  worse than no document, so the two are deliberately kept side by side.

  AsyncAPI 3.0 allows any schema format via the Multi Format Schema Object; this module
  emits JSON Schema draft 07, which is the AsyncAPI default and what tooling expects.
  """

  @doc """
  The JSON Schema for an outbound message payload.
  """
  @spec for_send(AshAsyncApi.Router.Table.ResolvedOperation.t()) :: map()
  def for_send(operation) do
    resource = operation.resource

    if AshAsyncApi.Resource.Info.derive_payload_schema?(resource) do
      fields = AshAsyncApi.Payload.send_fields(operation, resource)

      object(
        properties: Map.new(fields, &{&1, field_schema(resource, &1)}),
        required: required_send_fields(resource, fields),
        description: operation.operation.description
      )
    else
      %{"type" => "object"}
    end
  end

  @doc """
  The JSON Schema for an inbound message payload.

  Derived from the action's accepted attributes and arguments, so the document tells a
  producer exactly what the action will accept — including which parts are required.
  """
  @spec for_receive(AshAsyncApi.Router.Table.ResolvedOperation.t()) :: map()
  def for_receive(operation) do
    resource = operation.resource

    if AshAsyncApi.Resource.Info.derive_payload_schema?(resource) do
      fields = AshAsyncApi.Payload.receive_fields(operation)
      action = Ash.Resource.Info.action(resource, operation.action)

      object(
        properties: Map.new(fields, &{&1, input_field_schema(resource, action, &1)}),
        required: required_receive_fields(resource, action, fields, operation),
        description: operation.operation.description
      )
    else
      %{"type" => "object"}
    end
  end

  @doc """
  The JSON Schema for a single field of a resource.
  """
  @spec field_schema(module(), atom()) :: map()
  def field_schema(resource, field) do
    case Ash.Resource.Info.field(resource, field) do
      nil ->
        %{}

      definition ->
        definition
        |> type_schema()
        |> maybe_put("description", Map.get(definition, :description))
        |> maybe_nullable(definition)
    end
  end

  @doc """
  The JSON Schema for an Ash type and its constraints.
  """
  @spec type_schema(%{:type => term(), optional(any()) => any()}) :: map()
  def type_schema(%{type: type} = definition) do
    constraints = Map.get(definition, :constraints, []) || []

    do_type_schema(unwrap(type), constraints)
  end

  defp do_type_schema({:array, type}, constraints) do
    item_constraints = Keyword.get(constraints, :items, [])

    %{
      "type" => "array",
      "items" => do_type_schema(unwrap(type), item_constraints)
    }
    |> maybe_put("minItems", constraints[:min_length])
    |> maybe_put("maxItems", constraints[:max_length])
  end

  defp do_type_schema(Ash.Type.String, constraints) do
    %{"type" => "string"}
    |> maybe_put("minLength", constraints[:min_length])
    |> maybe_put("maxLength", constraints[:max_length])
    |> maybe_put("pattern", regex_source(constraints[:match]))
  end

  defp do_type_schema(Ash.Type.CiString, constraints),
    do: do_type_schema(Ash.Type.String, constraints)

  defp do_type_schema(Ash.Type.Atom, constraints) do
    case constraints[:one_of] do
      nil -> %{"type" => "string"}
      values -> %{"type" => "string", "enum" => Enum.map(values, &to_string/1)}
    end
  end

  defp do_type_schema(Ash.Type.Boolean, _constraints), do: %{"type" => "boolean"}

  defp do_type_schema(Ash.Type.Integer, constraints) do
    %{"type" => "integer"}
    |> maybe_put("minimum", constraints[:min])
    |> maybe_put("maximum", constraints[:max])
  end

  defp do_type_schema(Ash.Type.Float, constraints) do
    %{"type" => "number"}
    |> maybe_put("minimum", constraints[:min])
    |> maybe_put("maximum", constraints[:max])
  end

  # Decimals are dumped as strings to keep precision, so the schema has to say string.
  defp do_type_schema(Ash.Type.Decimal, _constraints) do
    %{"type" => "string", "format" => "decimal"}
  end

  defp do_type_schema(Ash.Type.Money, _constraints) do
    %{"type" => "string", "format" => "money"}
  end

  defp do_type_schema(Ash.Type.UUID, _constraints), do: %{"type" => "string", "format" => "uuid"}

  defp do_type_schema(Ash.Type.UUIDv7, _constraints),
    do: %{"type" => "string", "format" => "uuid"}

  defp do_type_schema(Ash.Type.Date, _constraints), do: %{"type" => "string", "format" => "date"}

  defp do_type_schema(Ash.Type.Time, _constraints), do: %{"type" => "string", "format" => "time"}

  defp do_type_schema(Ash.Type.TimeUsec, _constraints),
    do: %{"type" => "string", "format" => "time"}

  defp do_type_schema(type, _constraints)
       when type in [
              Ash.Type.UtcDatetime,
              Ash.Type.UtcDatetimeUsec,
              Ash.Type.DateTime,
              Ash.Type.NaiveDatetime
            ] do
    %{"type" => "string", "format" => "date-time"}
  end

  defp do_type_schema(Ash.Type.Duration, _constraints),
    do: %{"type" => "string", "format" => "duration"}

  defp do_type_schema(Ash.Type.DurationName, _constraints), do: %{"type" => "string"}

  defp do_type_schema(Ash.Type.Binary, _constraints),
    do: %{"type" => "string", "contentEncoding" => "base64"}

  defp do_type_schema(Ash.Type.File, _constraints),
    do: %{"type" => "string", "contentEncoding" => "base64"}

  defp do_type_schema(Ash.Type.Term, _constraints), do: %{}

  defp do_type_schema(Ash.Type.Function, _constraints), do: %{}

  defp do_type_schema(Ash.Type.Module, _constraints), do: %{"type" => "string"}

  defp do_type_schema(Ash.Type.Map, constraints), do: keyword_or_map_schema(constraints)

  defp do_type_schema(Ash.Type.Keyword, constraints), do: keyword_or_map_schema(constraints)

  defp do_type_schema(Ash.Type.Struct, constraints) do
    case constraints[:instance_of] do
      nil ->
        %{"type" => "object"}

      instance_of ->
        if Ash.Resource.Info.resource?(instance_of) do
          resource_schema(instance_of, constraints[:fields])
        else
          %{"type" => "object"}
        end
    end
  end

  defp do_type_schema(Ash.Type.Union, constraints) do
    case constraints[:types] do
      nil ->
        %{}

      types ->
        %{
          "oneOf" =>
            Enum.map(types, fn {_name, config} ->
              do_type_schema(unwrap(config[:type]), config[:constraints] || [])
            end)
        }
    end
  end

  defp do_type_schema(type, constraints) do
    cond do
      # Embedded resources and NewTypes wrapping resources describe themselves.
      Ash.Resource.Info.resource?(type) ->
        resource_schema(type, nil)

      function_exported?(type, :json_schema, 1) ->
        type.json_schema(constraints)

      Ash.Type.NewType.new_type?(type) ->
        do_type_schema(
          unwrap(Ash.Type.NewType.subtype_of(type)),
          Ash.Type.NewType.constraints(type, constraints)
        )

      Ash.Type.ash_type?(type) ->
        storage_type_schema(type, constraints)

      true ->
        %{}
    end
  rescue
    # A type that cannot describe itself should not stop the whole document from
    # generating; an empty schema means "anything", which is at least true.
    _ -> %{}
  end

  defp storage_type_schema(type, constraints) do
    case Ash.Type.storage_type(type, constraints) do
      :string -> %{"type" => "string"}
      :integer -> %{"type" => "integer"}
      :float -> %{"type" => "number"}
      :boolean -> %{"type" => "boolean"}
      :decimal -> %{"type" => "string", "format" => "decimal"}
      :map -> %{"type" => "object"}
      :uuid -> %{"type" => "string", "format" => "uuid"}
      :utc_datetime -> %{"type" => "string", "format" => "date-time"}
      :utc_datetime_usec -> %{"type" => "string", "format" => "date-time"}
      :date -> %{"type" => "string", "format" => "date"}
      _ -> %{}
    end
  end

  defp keyword_or_map_schema(constraints) do
    case constraints[:fields] do
      nil ->
        %{"type" => "object"}

      fields ->
        object(
          properties:
            Map.new(fields, fn {name, config} ->
              {name, do_type_schema(unwrap(config[:type]), config[:constraints] || [])}
            end),
          required: for({name, config} <- fields, config[:allow_nil?] == false, do: name)
        )
    end
  end

  defp resource_schema(resource, nil) do
    attributes = Ash.Resource.Info.public_attributes(resource)

    object(
      properties: Map.new(attributes, &{&1.name, field_schema(resource, &1.name)}),
      required: for(attribute <- attributes, not attribute.allow_nil?, do: attribute.name)
    )
  end

  defp resource_schema(resource, fields) do
    object(properties: Map.new(fields, &{&1, field_schema(resource, &1)}), required: [])
  end

  defp input_field_schema(resource, action, field) do
    argument = action && Enum.find(Map.get(action, :arguments, []), &(&1.name == field))

    if argument do
      argument
      |> type_schema()
      |> maybe_put("description", Map.get(argument, :description))
      |> maybe_nullable(argument)
    else
      field_schema(resource, field)
    end
  end

  # A field is required in an outbound payload when the resource guarantees it is there.
  defp required_send_fields(resource, fields) do
    Enum.filter(fields, fn field ->
      case Ash.Resource.Info.field(resource, field) do
        %Ash.Resource.Attribute{allow_nil?: false} -> true
        _ -> false
      end
    end)
  end

  # A field is required in an inbound payload when the action will reject the message
  # without it — but not when the address supplies it.
  defp required_receive_fields(resource, action, fields, operation) do
    from_address =
      case operation.compiled_address do
        nil -> []
        compiled -> compiled.params
      end

    Enum.filter(fields, fn field ->
      field not in from_address and required_input?(resource, action, field)
    end)
  end

  defp required_input?(_resource, nil, _field), do: false

  defp required_input?(resource, action, field) do
    case Enum.find(Map.get(action, :arguments, []), &(&1.name == field)) do
      %{allow_nil?: false} ->
        true

      nil ->
        case Ash.Resource.Info.attribute(resource, field) do
          %{allow_nil?: false, default: nil, generated?: false} ->
            field in (Map.get(action, :accept, []) || [])

          _ ->
            false
        end

      _ ->
        false
    end
  end

  defp object(opts) do
    properties = Keyword.get(opts, :properties, %{})
    required = Keyword.get(opts, :required, [])

    %{
      "type" => "object",
      "properties" => Map.new(properties, fn {name, schema} -> {to_string(name), schema} end)
    }
    |> maybe_put("required", required |> Enum.map(&to_string/1) |> presence())
    |> maybe_put("description", Keyword.get(opts, :description))
  end

  defp maybe_nullable(schema, %{allow_nil?: true, type: type}) when is_map(schema) do
    # `nil` is only reachable in a payload when nil values are kept, so declaring the
    # union unconditionally would over-describe. Only do it for scalars we typed.
    case Map.get(schema, "type") do
      nil -> schema
      _ when type == Ash.Type.Term -> schema
      declared -> Map.put(schema, "type", [declared, "null"])
    end
  end

  defp maybe_nullable(schema, _definition), do: schema

  defp unwrap({:array, type}), do: {:array, unwrap_scalar(type)}
  defp unwrap(type), do: unwrap_scalar(type)

  defp unwrap_scalar(type) do
    case Ash.Type.get_type(type) do
      {:array, inner} -> {:array, inner}
      resolved -> resolved
    end
  rescue
    _ -> type
  end

  defp regex_source(%Regex{} = regex), do: Regex.source(regex)
  defp regex_source(_), do: nil

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp presence([]), do: nil
  defp presence(list), do: list
end
