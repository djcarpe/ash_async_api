defmodule AshAsyncApi.Router do
  @moduledoc """
  The entry point for a running AsyncAPI application.

  A router ties one or more domains together, owns the `Group` instance that fans
  messages out across the cluster, supervises the transports that talk to your
  brokers, and generates the AsyncAPI document:

      defmodule Helpdesk.AsyncApiRouter do
        use AshAsyncApi.Router, domains: [Helpdesk.Support]
      end

  Add it to your supervision tree:

      children = [
        Helpdesk.Repo,
        Helpdesk.AsyncApiRouter
      ]

  From there, `publish/2` sends, `subscribe/1` receives, and `spec/0` describes:

      Helpdesk.AsyncApiRouter.subscribe(:ticket_events)
      Helpdesk.AsyncApiRouter.spec() |> Jason.encode!()

  ## Options

    * `:domains` (required) — the domains this router serves.
    * `:group` — options forwarded to `Group.start_link/1`, e.g `[shards: 16]`.
      `:shards` must match across every node in the cluster.
    * `:auto_subscribe?` — whether transports subscribe to the channels with
      `subscribe` operations on startup. Defaults to `true`. Set `false` to drive
      subscriptions yourself.
    * `:ignore_own_messages?` — whether inbound messages this router published are
      skipped, so that a channel used for both publishing and subscribing does not
      loop. Defaults to `true`. See the "Loops" section below.
    * `:start_transports?` — whether to start transports at all. Defaults to `true`;
      `false` gives you spec generation and in-cluster pub/sub with no broker
      connections, which is handy in tests and in mix tasks.

  ## Loops

  With `ignore_own_messages?: true` (the default), every published message carries an
  origin header naming the router and node that sent it. On the way in, a message is
  dropped if it came from the same router *and* from a node in the current cluster.
  Both halves matter: the router check stops a service from consuming its own events,
  and the node check means a second deployment of the same code — a blue/green
  cutover, say — still sees the other's messages, because its nodes are not in this
  cluster.
  """

  @doc """
  The routing table, built on first use and cached in `:persistent_term`.
  """
  @callback __ash_async_api__() :: AshAsyncApi.Router.Table.t()

  @doc """
  The router's configuration.
  """
  @callback __ash_async_api_config__() :: map()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts], location: :keep do
      @behaviour AshAsyncApi.Router

      @ash_async_api_domains opts |> Keyword.fetch!(:domains) |> List.wrap()
      @ash_async_api_config %{
        router: __MODULE__,
        domains: @ash_async_api_domains,
        group: Keyword.get(opts, :group, []),
        auto_subscribe?: Keyword.get(opts, :auto_subscribe?, true),
        ignore_own_messages?: Keyword.get(opts, :ignore_own_messages?, true),
        start_transports?: Keyword.get(opts, :start_transports?, true)
      }

      # Depend on each domain so that changing one recompiles the router, and with it
      # the cached routing table.
      for domain <- @ash_async_api_domains do
        @external_resource domain.module_info(:compile)[:source] |> to_string()
      end

      @impl AshAsyncApi.Router
      def __ash_async_api_config__, do: @ash_async_api_config

      @impl AshAsyncApi.Router
      def __ash_async_api__ do
        AshAsyncApi.Router.table(__MODULE__, @ash_async_api_domains)
      end

      @doc "The domains this router serves."
      def domains, do: @ash_async_api_domains

      @doc false
      def child_spec(overrides \\ []) do
        %{
          id: __MODULE__,
          start: {AshAsyncApi.Supervisor, :start_link, [__MODULE__, overrides]},
          type: :supervisor
        }
      end

      @doc "Start this router's supervision tree."
      def start_link(overrides \\ []) do
        AshAsyncApi.Supervisor.start_link(__MODULE__, overrides)
      end

      @doc """
      The AsyncAPI 3.0 document for this router, as a map.

      See `AshAsyncApi.Spec.generate/2` for options.
      """
      def spec(opts \\ []), do: AshAsyncApi.Spec.generate(__MODULE__, opts)

      @doc "The AsyncAPI document as pretty-printed JSON."
      def spec_json(opts \\ []), do: AshAsyncApi.Spec.to_json(__MODULE__, opts)

      @doc "The AsyncAPI document as YAML. Requires the `:ymlr` dependency."
      def spec_yaml(opts \\ []), do: AshAsyncApi.Spec.to_yaml(__MODULE__, opts)

      @doc """
      Subscribe the calling process to a channel or a concrete address.

      See `AshAsyncApi.subscribe/3`.
      """
      def subscribe(channel_or_address, meta \\ %{}) do
        AshAsyncApi.PubSub.subscribe(__MODULE__, channel_or_address, meta)
      end

      @doc "Unsubscribe the calling process."
      def unsubscribe(channel_or_address) do
        AshAsyncApi.PubSub.unsubscribe(__MODULE__, channel_or_address)
      end

      @doc """
      Publish a message. See `AshAsyncApi.publish/3`.
      """
      def publish(subject, opts \\ []), do: AshAsyncApi.publish(__MODULE__, subject, opts)

      @doc "The resolved channels this router knows about."
      def channels, do: __ash_async_api__().channels

      @doc "The resolved operations this router knows about."
      def operations, do: __ash_async_api__().operations
    end
  end

  @table_key_prefix {__MODULE__, :table}

  @doc """
  The routing table for a router, built once and cached in `:persistent_term`.

  Cached rather than compiled in because operations can hold anonymous functions
  (`transform`, `filter`), which cannot be escaped into a module attribute.
  """
  @spec table(module(), [module()]) :: AshAsyncApi.Router.Table.t()
  def table(router, domains) do
    key = {@table_key_prefix, router}

    case :persistent_term.get(key, nil) do
      nil ->
        built = AshAsyncApi.Router.Table.build(router, domains)
        :persistent_term.put(key, built)
        built

      table ->
        table
    end
  end

  @doc """
  Drop a router's cached table so the next read rebuilds it.

  Only needed when domains are redefined at runtime, which in practice means tests
  and `iex -S mix` sessions.
  """
  @spec clear_table(module()) :: :ok
  def clear_table(router) do
    :persistent_term.erase({@table_key_prefix, router})
    :ok
  end

  @doc """
  The config for a router.
  """
  @spec config(module()) :: map()
  def config(router), do: router.__ash_async_api_config__()
end
