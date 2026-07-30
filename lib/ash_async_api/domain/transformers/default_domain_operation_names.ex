defmodule AshAsyncApi.Domain.Transformers.DefaultDomainOperationNames do
  @moduledoc """
  Fills in operation ids and message names for domain-declared operations.

  The resource-level transformer cannot do this, because the resource does not know
  what its domain declared on its behalf.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    operations = Transformer.get_entities(dsl, [:async_api, :operations])
    ambiguous = ambiguous_actions(operations)

    dsl =
      Enum.reduce(operations, dsl, fn operation, dsl ->
        Transformer.replace_entity(
          dsl,
          [:async_api, :operations],
          fill_in(operation, ambiguous),
          &(&1.resource == operation.resource and &1.action == operation.action and
              &1.direction == operation.direction and &1.channel == operation.channel)
        )
      end)

    {:ok, dsl}
  end

  # The resource is compiled separately, so `Ash.Resource.Info` is safe to call
  # here — but only for the type name, which is a compile-time option.
  defp fill_in(operation, ambiguous) do
    base = "#{type(operation.resource)}_#{operation.action}"

    %{
      operation
      | name: operation.name || default_name(base, operation, ambiguous),
        # See the note in `AshAsyncApi.Resource.Transformers.DefaultOperationNames`:
        # the message name comes from the action, not the disambiguated operation id.
        message_name: operation.message_name || camelize(base)
    }
  end

  defp default_name(base, operation, ambiguous) do
    if MapSet.member?(ambiguous, {operation.resource, operation.action}) do
      :"#{base}_#{operation.direction}"
    else
      :"#{base}"
    end
  end

  # Only operations that would *derive* the same name collide. An explicit `name` takes
  # its sibling out of the running, so `publish` keeps the clean `comment_add` when the
  # matching `subscribe` was named by hand.
  defp ambiguous_actions(operations) do
    operations
    |> Enum.filter(&is_nil(&1.name))
    |> Enum.group_by(&{&1.resource, &1.action})
    |> Enum.filter(fn {_key, ops} -> length(ops) > 1 end)
    |> Enum.map(&elem(&1, 0))
    |> MapSet.new()
  end

  defp type(resource) do
    AshAsyncApi.Resource.Info.type(resource) ||
      resource |> Module.split() |> List.last() |> Macro.underscore()
  end

  defp camelize(name) do
    [first | rest] = name |> to_string() |> String.split("_", trim: true)

    Enum.join([first | Enum.map(rest, &String.capitalize/1)])
  end
end
