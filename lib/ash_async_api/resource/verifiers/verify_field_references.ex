defmodule AshAsyncApi.Resource.Verifiers.VerifyFieldReferences do
  @moduledoc """
  Checks that every field named in `hide_fields`, `show_fields`, `payload_fields`
  and `except_fields` actually exists.

  Catching a typo here is much cheaper than discovering a silently missing payload
  field in production.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    with :ok <- verify_section_fields(dsl, :hide_fields),
         :ok <- verify_section_fields(dsl, :show_fields) do
      verify_operation_fields(dsl)
    end
  end

  defp verify_section_fields(dsl, option) do
    case Verifier.get_option(dsl, [:async_api], option) do
      nil -> :ok
      fields -> check(dsl, fields, [:async_api, option], field_names(dsl))
    end
  end

  defp verify_operation_fields(dsl) do
    dsl
    |> Verifier.get_entities([:async_api, :operations])
    |> Enum.reduce_while(:ok, fn operation, :ok ->
      # `publish` payloads come from the record, `subscribe` payloads become
      # action input — so the set of legal names differs by direction.
      known =
        case operation.direction do
          :send -> field_names(dsl)
          :receive -> input_names(dsl, operation.action)
        end

      path = [:async_api, :operations, operation.direction, operation.action]

      with :ok <- check(dsl, operation.payload_fields, path ++ [:payload_fields], known),
           :ok <- check(dsl, operation.except_fields, path ++ [:except_fields], known) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp check(_dsl, nil, _path, _known), do: :ok
  defp check(_dsl, [], _path, _known), do: :ok

  defp check(dsl, fields, path, known) do
    case Enum.reject(fields, &(&1 in known)) do
      [] ->
        :ok

      unknown ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: path,
           message: """
           Unknown field(s) #{inspect(unknown)}.

           Available: #{inspect(Enum.sort(known))}
           """
         )}
    end
  end

  defp field_names(dsl) do
    Enum.map(Ash.Resource.Info.fields(dsl), & &1.name)
  end

  defp input_names(dsl, action_name) do
    case Ash.Resource.Info.action(dsl, action_name) do
      nil ->
        # VerifyActions reports the missing action; don't pile on.
        field_names(dsl)

      action ->
        arguments = Enum.map(Map.get(action, :arguments, []), & &1.name)
        accepted = Map.get(action, :accept, []) || []

        Enum.uniq(arguments ++ accepted ++ field_names(dsl))
    end
  end
end
