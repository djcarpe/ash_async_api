defmodule AshAsyncApi.Supervisor do
  @moduledoc """
  The supervision tree a router starts.

  Two layers, in order:

    1. The `Group` instance backing `AshAsyncApi.PubSub`. It comes first because
       transports deliver into it the moment they connect, and delivering into a
       `Group` that is not running would crash the transport.
    2. One child per server that has a transport.

  A transport that dies is restarted without disturbing the others or losing
  subscriptions, since subscriptions live in `Group` and are held by the subscribing
  processes, not by the transport.
  """

  use Supervisor

  require Logger

  alias AshAsyncApi.Transport.Context

  @doc """
  Start a router's supervision tree.

  `overrides` are merged over the router's compile-time options, which is how a test
  can start a router with `start_transports?: false`, and how an application supplies
  runtime server configuration:

      {MyApp.AsyncApiRouter, servers: [nats: [transport_opts: [...]]]}
      {MyApp.AsyncApiRouter, servers: [nats: :disabled]}
  """
  @spec start_link(module(), keyword()) :: Supervisor.on_start()
  def start_link(router, overrides \\ []) do
    Supervisor.start_link(__MODULE__, {router, overrides},
      name: Module.concat(router, "Supervisor")
    )
  end

  @impl true
  def init({router, overrides}) do
    config = router |> AshAsyncApi.Router.config() |> Map.merge(Map.new(overrides))

    children =
      [AshAsyncApi.PubSub.child_spec({router, config.group})] ++ transports(router, config)

    record_active_servers(router, config)

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  The transport children for a router.

  Exposed so an application can supervise transports itself — put them under your own
  supervisor, or start a subset of them, and leave the rest to the router.
  """
  @spec transport_children(module(), keyword()) :: [Supervisor.child_spec()]
  def transport_children(router, overrides \\ []) do
    config = router |> AshAsyncApi.Router.config() |> Map.merge(Map.new(overrides))

    transports(router, config)
  end

  defp transports(_router, %{start_transports?: false}), do: []

  defp transports(router, config) do
    table = router.__ash_async_api__()

    table.servers
    |> Map.values()
    |> Enum.filter(fn {_domain, server} -> server.transport end)
    |> Enum.flat_map(fn {domain, server} ->
      transport_child(router, table, config, domain, server)
    end)
  end

  defp transport_child(router, table, config, domain, server) do
    case server_config(config, server.name) do
      :disabled ->
        []

      runtime_opts ->
        server = apply_runtime_opts(server, runtime_opts)

        context =
          Context.new(router, domain, server,
            auto_subscribe?: config.auto_subscribe?,
            filters: subscription_filters(table, server)
          )

        case server.transport.child_spec(context) do
          nil -> []
          child -> [child]
        end
    end
  end

  defp server_config(config, server_name) do
    config |> Map.get(:servers, []) |> Keyword.get(server_name, [])
  end

  defp apply_runtime_opts(server, []), do: server

  defp apply_runtime_opts(server, runtime_opts) do
    %{
      server
      | transport_opts:
          Keyword.merge(server.transport_opts, Keyword.get(runtime_opts, :transport_opts, []))
    }
  end

  # Which servers this router actually started, for `AshAsyncApi.Publisher` to consult:
  # a publish to a server that was deliberately not started is a silent no-op, not an
  # error. Kept in `:persistent_term` because it changes only when the supervisor
  # (re)initializes, and the publisher reads it on every message.
  defp record_active_servers(router, config) do
    active =
      case config do
        %{start_transports?: false} ->
          MapSet.new()

        _config ->
          router.__ash_async_api__().servers
          |> Map.values()
          |> Enum.filter(fn {_domain, server} ->
            server.transport && server_config(config, server.name) != :disabled
          end)
          |> MapSet.new(fn {_domain, server} -> server.name end)
      end

    :persistent_term.put({AshAsyncApi, :active_servers, router}, active)
  end

  @doc """
  The names of the servers a router's supervisor actually started transports for.

  `nil` when the router's supervisor has never run — a table can be built and a spec
  generated without one, and in that case the publisher assumes every server is live.
  """
  @spec active_servers(module()) :: MapSet.t() | nil
  def active_servers(router) do
    :persistent_term.get({AshAsyncApi, :active_servers, router}, nil)
  end

  @doc """
  The subscription filters a server's transport should subscribe to.

  Each channel on the server that has at least one `subscribe` operation is turned
  into a filter in that transport's own wildcard syntax. This is the whole of the
  broker-specific subscription logic, and `AshAsyncApi.Address.to_filter/2` does the
  translating.
  """
  @spec subscription_filters(AshAsyncApi.Router.Table.t(), AshAsyncApi.Server.t()) :: [String.t()]
  def subscription_filters(table, server) do
    style = AshAsyncApi.Transport.wildcard_style(server.transport)

    table
    |> AshAsyncApi.Router.Table.channels_for_server(server.name)
    |> Enum.filter(fn channel ->
      channel.address &&
        AshAsyncApi.Router.Table.ResolvedChannel.operations(channel, :receive) != []
    end)
    |> Enum.map(&AshAsyncApi.Address.to_filter(&1.compiled, style))
    |> Enum.uniq()
  end
end
