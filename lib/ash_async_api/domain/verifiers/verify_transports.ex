defmodule AshAsyncApi.Domain.Verifiers.VerifyTransports do
  @moduledoc """
  Gives each transport a chance to reject its own configuration at compile time.

  A transport may export `validate_opts/2`, receiving the server and its
  `transport_opts`. That is where "you must supply a `:client_id`" belongs — the
  transport knows its own requirements, and finding out at compile time beats
  finding out when the supervisor fails to start.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    dsl
    |> Verifier.get_entities([:async_api, :servers])
    |> Enum.reject(&is_nil(&1.transport))
    |> Enum.reduce_while(:ok, fn server, :ok ->
      case validate(server) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, error(dsl, server, reason)}}
      end
    end)
  end

  defp validate(server) do
    Code.ensure_compiled!(server.transport)

    if function_exported?(server.transport, :validate_opts, 2) do
      server.transport.validate_opts(server, server.transport_opts)
    else
      :ok
    end
  end

  defp error(dsl, server, reason) do
    Spark.Error.DslError.exception(
      module: Verifier.get_persisted(dsl, :module),
      path: [:async_api, :servers, server.name],
      message: """
      Invalid configuration for #{inspect(server.transport)} on server \
      #{inspect(server.name)}:

      #{format(reason)}
      """
    )
  end

  defp format(reason) when is_binary(reason), do: reason
  defp format(%{message: message}) when is_binary(message), do: message
  defp format(reason), do: inspect(reason)
end
