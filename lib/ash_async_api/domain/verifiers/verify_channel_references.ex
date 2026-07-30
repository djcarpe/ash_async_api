defmodule AshAsyncApi.Domain.Verifiers.VerifyChannelReferences do
  @moduledoc """
  Checks that domain-declared operations reference a channel that exists, either on
  the domain or on the operation's resource.

  A resource may still be mid-compilation when its domain is verified, in which case
  the check is skipped here — `AshAsyncApi.Router` resolves every operation against
  its channel when it builds the routing table, so nothing slips through.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    domain_channels = MapSet.new(Verifier.get_entities(dsl, [:async_api, :channels]), & &1.name)

    dsl
    |> Verifier.get_entities([:async_api, :operations])
    |> Enum.reduce_while(:ok, fn operation, :ok ->
      case verify_operation(dsl, operation, domain_channels) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify_operation(dsl, operation, domain_channels) do
    cond do
      MapSet.member?(domain_channels, operation.channel) ->
        :ok

      resource_channel?(operation) ->
        :ok

      resource_compiled?(operation.resource) ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: [:async_api, :operations, operation.direction, operation.action],
           message: """
           Operation #{inspect(operation.name || operation.action)} references unknown \
           channel #{inspect(operation.channel)}.

           Channels on this domain: #{inspect(Enum.sort(domain_channels))}
           Channels on #{inspect(operation.resource)}: #{inspect(resource_channels(operation.resource))}
           """
         )}

      true ->
        # Resource not compiled yet; the router will catch it.
        :ok
    end
  end

  defp resource_channel?(operation) do
    operation.channel in resource_channels(operation.resource)
  end

  defp resource_channels(resource) do
    if resource_compiled?(resource) and AshAsyncApi.Resource.Info.async_api?(resource) do
      resource |> AshAsyncApi.Resource.Info.channels() |> Enum.map(& &1.name)
    else
      []
    end
  end

  defp resource_compiled?(resource) do
    match?({:module, _}, Code.ensure_compiled(resource))
  end
end
