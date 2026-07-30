defmodule AshAsyncApi.Resource.Verifiers.VerifyOperationNames do
  @moduledoc """
  Checks that operation ids and message names are unique within the resource.

  AsyncAPI keys operations and messages by name, so duplicates would silently
  overwrite each other in the generated document.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    operations = Verifier.get_entities(dsl, [:async_api, :operations])

    with :ok <- verify_unique(dsl, operations, & &1.name, "operation name") do
      verify_unique(dsl, operations, &{&1.channel, &1.message_name}, "message name")
    end
  end

  defp verify_unique(dsl, operations, key_fun, label) do
    operations
    |> Enum.group_by(key_fun)
    |> Enum.find(fn {_key, group} -> length(group) > 1 end)
    |> case do
      nil ->
        :ok

      {key, group} ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: [:async_api, :operations],
           message: """
           Duplicate #{label} #{inspect(describe_key(key))}, used by #{describe(group)}.

           Set a distinct `name` or `message_name` on one of them.
           """
         )}
    end
  end

  defp describe_key({_channel, message_name}), do: message_name
  defp describe_key(key), do: key

  defp describe(operations) do
    Enum.map_join(operations, ", ", fn operation ->
      "#{operation.direction} #{inspect(operation.action)}, #{inspect(operation.channel)}"
    end)
  end
end
