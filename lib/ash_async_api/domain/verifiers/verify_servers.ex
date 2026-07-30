defmodule AshAsyncApi.Domain.Verifiers.VerifyServers do
  @moduledoc """
  Checks that server names are unique, that `default_server` names a real server,
  and that every channel's `servers` list resolves.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    servers = Verifier.get_entities(dsl, [:async_api, :servers])
    names = MapSet.new(servers, & &1.name)

    with :ok <- verify_unique(dsl, servers),
         :ok <- verify_default(dsl, names) do
      verify_channel_servers(dsl, names)
    end
  end

  defp verify_unique(dsl, servers) do
    servers
    |> Enum.frequencies_by(& &1.name)
    |> Enum.find(fn {_name, count} -> count > 1 end)
    |> case do
      nil ->
        :ok

      {name, _count} ->
        {:error,
         error(dsl, [:async_api, :servers, name],
           message: "Multiple servers are named #{inspect(name)}. Server names must be unique."
         )}
    end
  end

  defp verify_default(dsl, names) do
    case Verifier.get_option(dsl, [:async_api], :default_server) do
      nil ->
        :ok

      default ->
        if MapSet.member?(names, default) do
          :ok
        else
          {:error,
           error(dsl, [:async_api, :default_server],
             message: """
             `default_server #{inspect(default)}` does not match any declared server.

             Declared servers: #{inspect(Enum.sort(names))}
             """
           )}
        end
    end
  end

  defp verify_channel_servers(dsl, names) do
    dsl
    |> Verifier.get_entities([:async_api, :channels])
    |> Enum.reduce_while(:ok, fn channel, :ok ->
      case Enum.reject(channel.servers, &MapSet.member?(names, &1)) do
        [] ->
          {:cont, :ok}

        unknown ->
          {:halt,
           {:error,
            error(dsl, [:async_api, :channels, channel.name, :servers],
              message: """
              Channel #{inspect(channel.name)} references unknown server(s) #{inspect(unknown)}.

              Declared servers: #{inspect(Enum.sort(names))}
              """
            )}}
      end
    end)
  end

  defp error(dsl, path, opts) do
    Spark.Error.DslError.exception(
      [module: Verifier.get_persisted(dsl, :module), path: path] ++ opts
    )
  end
end
