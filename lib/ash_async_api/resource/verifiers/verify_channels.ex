defmodule AshAsyncApi.Resource.Verifiers.VerifyChannels do
  @moduledoc """
  Checks that declared channel parameters line up with the channel address, and
  that channel names are unique.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    channels = Verifier.get_entities(dsl, [:async_api, :channels])

    with :ok <- verify_unique_names(dsl, channels) do
      Enum.reduce_while(channels, :ok, fn channel, :ok ->
        case verify_channel(dsl, channel) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp verify_unique_names(dsl, channels) do
    channels
    |> Enum.frequencies_by(& &1.name)
    |> Enum.find(fn {_name, count} -> count > 1 end)
    |> case do
      nil ->
        :ok

      {name, _count} ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: [:async_api, :channels, name],
           message: "Multiple channels are named #{inspect(name)}. Channel names must be unique."
         )}
    end
  end

  defp verify_channel(_dsl, %{address: nil}), do: :ok

  defp verify_channel(dsl, channel) do
    address_params = AshAsyncApi.Address.params(channel.address)
    declared = Enum.map(channel.parameters, & &1.name)

    case declared -- address_params do
      [] ->
        :ok

      undeclared ->
        {:error,
         Spark.Error.DslError.exception(
           module: Verifier.get_persisted(dsl, :module),
           path: [:async_api, :channels, channel.name],
           message: """
           Channel #{inspect(channel.name)} declares parameter(s) #{inspect(undeclared)} \
           that do not appear in its address.

               address: #{inspect(channel.address)}
               address parameters: #{inspect(address_params)}

           Wrap the segment in braces to make it a parameter, e.g \
           #{inspect(example_address(channel.address, hd(undeclared)))}.
           """
         )}
    end
  end

  defp example_address(address, param) do
    separator = if String.contains?(address, "."), do: ".", else: "/"
    address <> separator <> "{#{param}}"
  end
end
