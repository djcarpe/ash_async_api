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
  can start a router with `start_transports?: false`.
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
      context =
        Context.new(router, domain, server,
          auto_subscribe?: config.auto_subscribe?,
          filters: subscription_filters(table, server)
        )

      case server.transport.child_spec(context) do
        nil -> []
        child -> [child]
      end
    end)
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
