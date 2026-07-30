defmodule AshAsyncApi.PubSub do
  @moduledoc """
  Cluster-wide, provider-independent fan-out, built on
  [`Group`](https://group.hexdocs.pm/Group.html).

  This is the seam that keeps AshAsyncApi's runtime from caring which broker is in
  play. A transport connection lives on exactly one node — whichever one happens to
  hold the MQTT socket or the Kafka consumer — but the processes that want the
  message could be anywhere in the cluster. So inbound messages are handed to
  `Group`, which fans them out over Erlang distribution to every subscriber on every
  node.

      MQTT broker ──▶ transport (node A) ──▶ AshAsyncApi.PubSub ──▶ subscribers (nodes A, B, C)

  Swap MQTT for NATS or Kafka and everything to the right of the transport is
  unchanged. That is the whole point: `Group` is the provider abstraction.

  ## Key spaces

  Subscriptions live in two key spaces, and a broadcast hits both:

    * `channel/<domain>/<channel>` — every message on a channel, whatever the
      concrete address.
    * `address/<address>` — only messages at one concrete address, e.g
      `address/helpdesk/tickets/42/events`.

  The second is what makes per-entity subscriptions cheap. A LiveView showing one
  ticket subscribes to that ticket's address and is never woken by traffic for other
  tickets.

  ## Delivery semantics

  Subscribers receive `{:ash_async_api, %AshAsyncApi.Envelope{}}`.

  `Group` is eventually consistent: a `subscribe/3` returns as soon as the local
  node is updated, and other nodes learn about it asynchronously. In practice this
  means a subscription made on node A may miss a message broadcast from node B in
  the milliseconds right after subscribing. For state that must not miss anything,
  subscribe and *then* read current state — the standard subscribe-then-fetch
  pattern.
  """

  require Logger

  @doc """
  The `Group` instance name for a router.

  Each router gets its own `Group` instance so that two routers in one application
  cannot see each other's subscriptions.
  """
  @spec group_name(module()) :: atom()
  def group_name(router), do: Module.concat(router, "PubSub")

  @doc """
  The child spec for the `Group` instance backing a router.

  ## Options

  Any option `Group.start_link/1` accepts, notably `:shards` (which must match
  across all nodes in the cluster).
  """
  @spec child_spec({module(), keyword()}) :: Supervisor.child_spec()
  def child_spec({router, opts}) do
    opts =
      opts
      |> Keyword.put(:name, group_name(router))
      |> Keyword.put_new(:log, false)

    %{
      id: {__MODULE__, router},
      start: {Group, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Subscribe the calling process to a channel.

  `meta` is stored alongside the subscription and is visible to
  `subscribers/2`, which is how you filter a fan-out without waking every
  process — see `Group.members/3`.
  """
  @spec subscribe(module(), atom() | String.t(), map()) :: :ok | {:error, term()}
  def subscribe(router, channel, meta \\ %{})

  def subscribe(router, channel, meta) when is_atom(channel) and not is_nil(channel) do
    join(router, channel_key(router, channel), meta)
  end

  def subscribe(router, address, meta) when is_binary(address) do
    join(router, address_key(address), meta)
  end

  @doc """
  Unsubscribe the calling process from a channel or address.
  """
  @spec unsubscribe(module(), atom() | String.t()) :: :ok | {:error, term()}
  def unsubscribe(router, channel) when is_atom(channel) and not is_nil(channel) do
    Group.leave(group_name(router), channel_key(router, channel))
  end

  def unsubscribe(router, address) when is_binary(address) do
    Group.leave(group_name(router), address_key(address))
  end

  @doc """
  Broadcast an envelope to every subscriber in the cluster.

  Delivers to both the channel key and the concrete address key, so channel-wide
  and address-specific subscribers are both served by one call.
  """
  @spec broadcast(module(), AshAsyncApi.Envelope.t()) :: :ok
  def broadcast(router, %AshAsyncApi.Envelope{} = envelope) do
    do_broadcast(router, envelope, &Group.dispatch/3)
  end

  @doc """
  Like `broadcast/2`, but only to subscribers on the local node.

  Use this when every node is already receiving the message from the broker
  independently — a Kafka consumer group with a member per node, for instance.
  Broadcasting cluster-wide in that setup would deliver N copies to each subscriber.
  """
  @spec broadcast_local(module(), AshAsyncApi.Envelope.t()) :: :ok
  def broadcast_local(router, %AshAsyncApi.Envelope{} = envelope) do
    do_broadcast(router, envelope, &Group.dispatch_local/3)
  end

  defp do_broadcast(router, envelope, dispatch) do
    group = group_name(router)

    # Notifications fire after the transaction commits, so raising here would blow up
    # a caller whose write already succeeded. A misconfigured supervision tree deserves
    # a loud log, not a crashed action.
    if running?(router) do
      message = {:ash_async_api, envelope}

      for key <- broadcast_keys(router, envelope) do
        dispatch.(group, key, message)
      end
    else
      warn_not_running(router, envelope)
    end

    :ok
  end

  defp warn_not_running(router, envelope) do
    Logger.error("""
    AshAsyncApi could not broadcast a message on #{inspect(envelope.channel)} because \
    #{inspect(group_name(router))} is not running, so no subscriber received it.

    #{inspect(router)} needs to be in your supervision tree:

        children = [
          # ...
          #{inspect(router)}
        ]
    """)
  end

  defp broadcast_keys(router, envelope) do
    channel_keys =
      case envelope.channel do
        nil -> []
        channel -> [channel_key(router, channel)]
      end

    address_keys =
      case envelope.address do
        nil -> []
        address -> [address_key(address)]
      end

    channel_keys ++ address_keys
  end

  @doc """
  The subscribers to a channel or address, as `{pid, meta}` pairs.

  Pass a channel/address to list its subscribers, or a string ending in `/` to do a
  prefix query — `"address/helpdesk/tickets/"` finds subscribers to every ticket.
  """
  @spec subscribers(module(), atom() | String.t()) :: [{pid(), map()}]
  def subscribers(router, channel) when is_atom(channel) and not is_nil(channel) do
    Group.members(group_name(router), channel_key(router, channel))
  end

  def subscribers(router, key) when is_binary(key) do
    if String.ends_with?(key, "/") do
      Group.members(group_name(router), key)
    else
      Group.members(group_name(router), address_key(key))
    end
  end

  @doc """
  How many subscribers a channel or address has, across the cluster.

  Cheaper than `subscribers/2` when you only need to know whether anyone is
  listening — worth checking before doing expensive work to build a payload.
  """
  @spec subscriber_count(module(), atom() | String.t()) :: non_neg_integer()
  def subscriber_count(router, channel) when is_atom(channel) and not is_nil(channel) do
    Group.member_count(group_name(router), channel_key(router, channel))
  end

  def subscriber_count(router, address) when is_binary(address) do
    Group.member_count(group_name(router), address_key(address))
  end

  @doc """
  The nodes participating in this router's `Group` instance.
  """
  @spec nodes(module()) :: [node()]
  def nodes(router), do: Group.nodes(group_name(router))

  @doc """
  Whether the `Group` instance for this router is running.

  Worth checking before a batch of publishes, since a router missing from the
  supervision tree silently drops every message.
  """
  @spec running?(module()) :: boolean()
  def running?(router) do
    not is_nil(Group.get_config(group_name(router)))
  end

  @doc false
  @spec channel_key(module(), atom()) :: String.t()
  def channel_key(router, channel) do
    "channel/#{inspect(router)}/#{channel}"
  end

  @doc false
  @spec address_key(String.t()) :: String.t()
  def address_key(address) do
    # Group reserves a trailing "/" for prefix queries, so it can never appear in
    # a key we join or dispatch to.
    "address/" <> String.trim_trailing(address, "/")
  end

  defp join(router, key, meta) do
    Group.join(group_name(router), key, meta)
  rescue
    error ->
      Logger.error("""
      AshAsyncApi could not subscribe to #{inspect(key)}: #{Exception.message(error)}

      Is #{inspect(group_name(router))} running? It is started by \
      #{inspect(router)}, which needs to be in your supervision tree.
      """)

      {:error, error}
  end
end
