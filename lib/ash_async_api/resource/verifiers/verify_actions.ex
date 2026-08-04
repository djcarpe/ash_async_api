defmodule AshAsyncApi.Resource.Verifiers.VerifyActions do
  @moduledoc """
  Checks that every operation refers to an action that exists, and that the action
  can actually do what the operation direction asks of it.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    dsl
    |> Verifier.get_entities([:async_api, :operations])
    |> Enum.reduce_while(:ok, fn operation, :ok ->
      case verify_operation(dsl, operation) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # A `publish_all` names an action *type*, constrained by its schema — there is no
  # single action to check, and matching no actions at all is legal (a resource with no
  # destroy actions simply never publishes a destroy).
  defp verify_operation(_dsl, %{all?: true}), do: :ok

  defp verify_operation(dsl, operation) do
    case Ash.Resource.Info.action(dsl, operation.action) do
      nil ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: [:async_api, :operations, operation.direction, operation.action],
           message: """
           No such action #{inspect(operation.action)}.

           Available actions: #{inspect(available_actions(dsl))}
           """
         )}

      action ->
        verify_direction(dsl, operation, action)
    end
  end

  # A read action produces a list, not a single record, so it has nothing sensible
  # to publish off a notification. Publishing one manually is fine.
  defp verify_direction(dsl, %{direction: :send} = operation, %{type: :read}) do
    if Verifier.get_option(dsl, [:async_api], :publish_on_notification?, true) do
      {:error,
       Spark.Error.DslError.exception(
         module: Verifier.get_persisted(dsl, :module),
         path: [:async_api, :operations, :publish, operation.action],
         message: """
         Cannot publish from read action #{inspect(operation.action)} automatically, \
         because read actions do not emit notifications.

         Either publish from a create/update/destroy action, or set \
         `publish_on_notification? false` and publish manually with \
         `AshAsyncApi.publish/3`.
         """
       )}
    else
      :ok
    end
  end

  defp verify_direction(_dsl, _operation, _action), do: :ok

  defp available_actions(dsl) do
    dsl |> Ash.Resource.Info.actions() |> Enum.map(& &1.name)
  end
end
