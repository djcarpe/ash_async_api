defmodule AshAsyncApi.Payload do
  @moduledoc """
  Turning records into message payloads, and message payloads into action input.

  Outbound, this decides which fields go on the wire and dumps their values to
  JSON-safe terms via the Ash type system, so a `Decimal`, a `DateTime` or a custom
  type all serialize the way the generated JSON Schema says they will.

  Inbound, it does the reverse: narrows an arbitrary map down to the fields the
  action will accept, which keeps a message from setting attributes it has no
  business setting.
  """

  alias AshAsyncApi.Router.Table.ResolvedOperation

  @doc """
  Build the payload for an outbound message from a record.

  Field selection is `payload_fields` (defaulting to the resource's public
  attributes), minus `except_fields`, minus the resource's `hide_fields`, intersected
  with `show_fields` when set. `hide_fields` is applied last so a hidden field cannot
  be re-exposed by naming it in `payload_fields`.
  """
  @spec for_send(ResolvedOperation.t(), Ash.Resource.record()) :: {:ok, map()} | {:error, term()}
  def for_send(%ResolvedOperation{} = operation, record) do
    resource = operation.resource
    fields = send_fields(operation, resource)

    payload =
      fields
      |> Enum.reduce(%{}, fn field, acc ->
        case fetch_field(record, field) do
          {:ok, value} -> Map.put(acc, field, dump(resource, field, value))
          :error -> acc
        end
      end)
      |> drop_nils(resource)

    case operation.operation.transform do
      nil -> {:ok, payload}
      transform -> apply_transform(transform, payload, record)
    end
  end

  @doc """
  Build the action input for an inbound message.

  Keys may arrive as strings or atoms and in camelCase; both are matched against the
  action's accepted attributes and arguments. Anything unrecognised is dropped rather
  than passed through, so a message cannot reach an attribute the action does not
  accept.
  """
  @spec for_receive(ResolvedOperation.t(), term(), AshAsyncApi.Envelope.t()) ::
          {:ok, map()} | {:error, term()}
  def for_receive(%ResolvedOperation{} = operation, payload, envelope) do
    with {:ok, payload} <- apply_receive_transform(operation, payload, envelope) do
      {:ok, cast_input(operation, payload, envelope)}
    end
  end

  defp apply_receive_transform(%{operation: %{transform: nil}}, payload, _envelope),
    do: {:ok, payload}

  defp apply_receive_transform(%{operation: %{transform: transform}}, payload, envelope) do
    apply_transform(transform, payload, envelope)
  end

  defp apply_transform(transform, payload, subject) do
    case transform.(payload, subject) do
      %{} = transformed -> {:ok, transformed}
      {:ok, transformed} -> {:ok, transformed}
      {:error, reason} -> {:error, reason}
      other -> {:error, "expected the transform to return a map, got: #{inspect(other)}"}
    end
  end

  defp cast_input(operation, payload, envelope) when is_map(payload) do
    accepted = receive_fields(operation)
    lookup = build_lookup(payload)

    accepted
    |> Enum.reduce(%{}, fn field, acc ->
      case fetch_from_lookup(lookup, field) do
        {:ok, value} -> Map.put(acc, field, value)
        :error -> acc
      end
    end)
    # Address parameters are part of the message's identity, not its body — a
    # message at `tickets/42/commands` is about ticket 42 whatever the payload says.
    |> merge_address_params(operation, envelope)
  end

  defp cast_input(_operation, payload, _envelope), do: %{payload: payload}

  defp merge_address_params(input, operation, %{params: params}) when map_size(params) > 0 do
    accepted = receive_fields(operation)

    params
    |> Enum.filter(fn {name, _value} -> name in accepted end)
    |> Enum.reduce(input, fn {name, value}, acc -> Map.put_new(acc, name, value) end)
  end

  defp merge_address_params(input, _operation, _envelope), do: input

  @doc """
  The fields that will appear in an outbound payload.

  Exposed because the spec generator needs exactly the same answer as the runtime —
  a JSON Schema that disagrees with the messages actually sent is worse than no
  schema at all.
  """
  @spec send_fields(ResolvedOperation.t(), module()) :: [atom()]
  def send_fields(%ResolvedOperation{operation: operation}, resource) do
    declared = operation.payload_fields || default_send_fields(resource)

    declared
    |> Enum.reject(&(&1 in (operation.except_fields || [])))
    |> Enum.filter(&AshAsyncApi.Resource.Info.show_field?(resource, &1))
    |> Enum.uniq()
  end

  @doc """
  The fields an inbound payload may set.
  """
  @spec receive_fields(ResolvedOperation.t()) :: [atom()]
  def receive_fields(%ResolvedOperation{operation: operation, resource: resource, action: action}) do
    declared = operation.payload_fields || default_receive_fields(resource, action)
    excluded = (operation.except_fields || []) ++ AshAsyncApi.Resource.Info.hide_fields(resource)

    declared
    |> Enum.reject(&(&1 in excluded))
    |> Enum.uniq()
  end

  defp default_send_fields(resource) do
    resource
    |> Ash.Resource.Info.public_attributes()
    |> Enum.map(& &1.name)
  end

  defp default_receive_fields(resource, action_name) do
    case Ash.Resource.Info.action(resource, action_name) do
      nil ->
        []

      action ->
        arguments = action |> Map.get(:arguments, []) |> Enum.map(& &1.name)
        accepted = accepted_attributes(resource, action)

        Enum.uniq(accepted ++ arguments)
    end
  end

  defp accepted_attributes(resource, %{type: :read}), do: primary_key_fields(resource)

  defp accepted_attributes(_resource, %{accept: accept}) when is_list(accept), do: accept

  defp accepted_attributes(resource, %{type: type}) when type in [:update, :destroy] do
    primary_key_fields(resource)
  end

  defp accepted_attributes(_resource, _action), do: []

  defp primary_key_fields(resource), do: Ash.Resource.Info.primary_key(resource)

  defp fetch_field(record, field) do
    case Map.fetch(record, field) do
      {:ok, %Ash.NotLoaded{}} -> :error
      {:ok, %Ash.ForbiddenField{}} -> :error
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  defp drop_nils(payload, resource) do
    if AshAsyncApi.Resource.Info.include_nil_values?(resource) do
      payload
    else
      Map.reject(payload, fn {_key, value} -> is_nil(value) end)
    end
  end

  defp dump(resource, field, value) do
    case field_type(resource, field) do
      nil ->
        json_safe(value)

      {type, constraints} ->
        case Ash.Type.dump_to_embedded(type, value, constraints) do
          {:ok, dumped} -> json_safe(dumped)
          _ -> json_safe(value)
        end
    end
  end

  @doc """
  Coerce a dumped value into something JSON can represent directly.

  `Ash.Type.dump_to_embedded/3` stops short of this: it leaves a `DateTime` as a
  `DateTime`, on the assumption that whatever serializes next will handle it. That is
  not good enough here, because the same envelope is delivered two ways — straight to
  local subscribers, and encoded through a broker to remote ones — and a subscriber must
  not see a `DateTime` in dev and a string in production. Normalizing here also makes
  the payload match the JSON Schema that `AshAsyncApi.Spec.Schema` published for it.
  """
  @spec json_safe(term()) :: term()
  def json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  def json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  def json_safe(%Date{} = value), do: Date.to_iso8601(value)
  def json_safe(%Time{} = value), do: Time.to_iso8601(value)
  def json_safe(%Decimal{} = value), do: Decimal.to_string(value)
  def json_safe(%Ash.NotLoaded{}), do: nil
  def json_safe(%Ash.ForbiddenField{}), do: nil

  def json_safe(%struct{} = value) when struct not in [Ash.Union] do
    if Ash.Resource.Info.resource?(struct) do
      value
      |> Map.take(Enum.map(Ash.Resource.Info.public_attributes(struct), & &1.name))
      |> json_safe()
    else
      value |> Map.from_struct() |> json_safe()
    end
  end

  def json_safe(value) when is_map(value) do
    Map.new(value, fn {key, value} -> {key, json_safe(value)} end)
  end

  def json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  def json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()
  def json_safe(value), do: value

  defp field_type(resource, field) do
    case Ash.Resource.Info.field(resource, field) do
      %{type: type} = definition -> {type, Map.get(definition, :constraints, [])}
      _ -> nil
    end
  end

  # Messages from other systems arrive in whatever case that system uses. Matching
  # atom, string and camelCase forms means an AsyncAPI document written by someone
  # else can be consumed without a transform for every field.
  defp build_lookup(payload) do
    Enum.reduce(payload, %{}, fn {key, value}, acc ->
      acc
      |> Map.put(to_string(key), value)
      |> Map.put(underscore(to_string(key)), value)
    end)
  end

  defp fetch_from_lookup(lookup, field) do
    name = Atom.to_string(field)

    case Map.fetch(lookup, name) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(lookup, underscore(name))
    end
  end

  defp underscore(string) do
    string
    |> Macro.underscore()
    |> String.replace("-", "_")
  end
end
