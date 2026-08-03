defmodule AshAsyncApi.Resource.Transformers.DefaultOperationNames do
  @moduledoc """
  Fills in the derivable parts of each operation.

  Operation ids default to `<type>_<action>` (suffixed with the direction when a
  `publish` and a `subscribe` share an action), message names default to the
  camelized operation id, and `resource`/`action_type` are stamped on so the
  runtime does not have to look them up.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    type = Transformer.get_option(dsl, [:async_api], :type)
    resource = Transformer.get_persisted(dsl, :module)
    operations = Transformer.get_entities(dsl, [:async_api, :operations])
    ambiguous = ambiguous_actions(operations)

    # `publish_all` operations have no action to derive names from; they are named per
    # expanded action when the routing table is built.
    dsl =
      operations
      |> Enum.reject(& &1.all?)
      |> Enum.reduce(dsl, fn operation, dsl ->
        Transformer.replace_entity(
          dsl,
          [:async_api, :operations],
          fill_in(operation, type, resource, dsl, ambiguous),
          &(not &1.all? and &1.action == operation.action and
              &1.direction == operation.direction)
        )
      end)

    {:ok, dsl}
  end

  @impl true
  def after?(AshAsyncApi.Resource.Transformers.SetType), do: true
  def after?(_), do: false

  # An action bound to both a publish and a subscribe cannot share one operation id — but
  # only operations that *derive* their name collide. An explicit `name` on one takes it
  # out of the running, so the other keeps the clean default.
  defp ambiguous_actions(operations) do
    operations
    |> Enum.reject(& &1.all?)
    |> Enum.filter(&is_nil(&1.name))
    |> Enum.group_by(& &1.action)
    |> Enum.filter(fn {_action, ops} -> length(ops) > 1 end)
    |> Enum.map(&elem(&1, 0))
    |> MapSet.new()
  end

  defp fill_in(operation, type, resource, dsl, ambiguous) do
    base = "#{type}_#{operation.action}"

    %{
      operation
      | name: operation.name || default_name(base, operation, ambiguous),
        resource: operation.resource || resource,
        action_type: action_type(dsl, operation.action),
        # Derived from the action rather than from the (possibly disambiguated)
        # operation id, so a `publish`/`subscribe` pair does not produce a message
        # called `ticketOpenSend`. Message names only have to be unique per channel,
        # and the two directions of one action are on different channels in practice —
        # `AshAsyncApi.Resource.Verifiers.VerifyOperationNames` enforces that.
        message_name: operation.message_name || camelize(base)
    }
  end

  defp default_name(base, operation, ambiguous) do
    if MapSet.member?(ambiguous, operation.action) do
      :"#{base}_#{operation.direction}"
    else
      :"#{base}"
    end
  end

  defp action_type(dsl, action_name) do
    case Ash.Resource.Info.action(dsl, action_name) do
      nil -> nil
      action -> action.type
    end
  end

  defp camelize(name) do
    [first | rest] = name |> to_string() |> String.split("_", trim: true)

    Enum.join([first | Enum.map(rest, &String.capitalize/1)])
  end
end
