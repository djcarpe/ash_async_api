defmodule AshAsyncApi.Publisher do
  @moduledoc """
  The outbound path: record in, message on the wire.

  Publishing does two things for every message, in this order:

    1. Broadcasts the envelope through `AshAsyncApi.PubSub`, so every subscribed
       process in the cluster sees it immediately, whether or not a broker is
       involved.
    2. Hands the encoded envelope to each transport bound to the channel.

  Doing (1) unconditionally is what makes `AshAsyncApi.subscribe/2` reliable: a
  LiveView watching a ticket does not have to wait for a round trip through MQTT to
  learn that the ticket changed, and it keeps working in tests where no broker exists.

  The origin header stamped on every message is what stops (2) from undoing (1) — see
  the "Loops" section of `AshAsyncApi.Router`.
  """

  require Logger

  alias AshAsyncApi.Envelope
  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Router.Table.ResolvedOperation
  alias AshAsyncApi.Transport.Context

  @origin_header "ash-async-api-origin"

  @doc false
  def origin_header, do: @origin_header

  @doc """
  Publish every `send` operation bound to a resource action for one record.

  Returns the envelopes that were published. An operation whose `filter` returns
  `false` is skipped and contributes no envelope.
  """
  @spec publish_record(module(), Ash.Resource.record(), keyword()) ::
          {:ok, [Envelope.t()]} | {:error, term()}
  def publish_record(router, %resource{} = record, opts) do
    table = router.__ash_async_api__()
    action = Keyword.fetch!(opts, :action)

    case Table.operations_for(table, resource, action, :send) do
      [] ->
        {:error, unknown_operation_error(router, table, resource, action)}

      operations ->
        publish_operations(router, table, operations, record, opts)
    end
  end

  @doc """
  Publish for an `Ash.Notifier.Notification`.

  The notification's data is the record, and its action tells us which operations
  apply. Unlike `publish_record/3`, an action with no matching operation is not an
  error — most actions on a resource are not published, and the notifier sees all of
  them.
  """
  @spec publish_notification(module(), Ash.Notifier.Notification.t()) ::
          {:ok, [Envelope.t()]} | {:error, term()}
  def publish_notification(router, %Ash.Notifier.Notification{} = notification) do
    table = router.__ash_async_api__()
    resource = notification.resource
    action = notification.action.name

    case Table.operations_for(table, resource, action, :send) do
      [] ->
        {:ok, []}

      operations ->
        publish_operations(router, table, operations, notification.data,
          notification: notification,
          actor: notification.actor,
          tenant: notification.metadata[:tenant]
        )
    end
  end

  @doc """
  Publish an arbitrary payload onto a channel, with no operation involved.

  For messages that are not tied to a resource action — a heartbeat, an audit
  record, a reply. `params` supplies any address parameters the channel needs.

      AshAsyncApi.Publisher.publish_to(MyRouter, :ticket_events, %{hello: "world"},
        params: %{ticket_id: 42}
      )
  """
  @spec publish_to(module(), atom(), term(), keyword()) :: {:ok, Envelope.t()} | {:error, term()}
  def publish_to(router, channel_key, payload, opts \\ []) do
    table = router.__ash_async_api__()

    case Table.channel(table, channel_key) do
      nil ->
        {:error,
         AshAsyncApi.Error.UnknownChannel.exception(
           channel: channel_key,
           resource: nil,
           domain: hd(table.domains),
           known: Enum.map(table.channels, & &1.key)
         )}

      channel ->
        with {:ok, address} <- address_for(channel, Keyword.get(opts, :params, %{})) do
          envelope =
            Envelope.new(
              channel: channel.key,
              address: address,
              message: opts[:message],
              payload: payload,
              headers: Map.new(opts[:headers] || %{}),
              content_type:
                opts[:content_type] ||
                  AshAsyncApi.Domain.Info.default_content_type(channel.domain),
              correlation_id: opts[:correlation_id],
              reply_to: opts[:reply_to],
              params: Keyword.get(opts, :params, %{}),
              router: router
            )

          deliver(router, table, channel, envelope, opts)
        end
    end
  end

  @doc """
  Publish an already-built envelope.

  The envelope must name a channel this router knows. Used for replies, and for
  re-publishing a message received elsewhere.
  """
  @spec publish_envelope(module(), Envelope.t(), keyword()) ::
          {:ok, Envelope.t()} | {:error, term()}
  def publish_envelope(router, %Envelope{} = envelope, opts \\ []) do
    table = router.__ash_async_api__()

    case Table.channel(table, envelope.channel) do
      nil ->
        {:error,
         AshAsyncApi.Error.UnknownChannel.exception(
           channel: envelope.channel,
           resource: envelope.resource,
           domain: hd(table.domains),
           known: Enum.map(table.channels, & &1.key)
         )}

      channel ->
        with {:ok, address} <- resolve_envelope_address(channel, envelope) do
          deliver(router, table, channel, %{envelope | address: address, router: router}, opts)
        end
    end
  end

  defp resolve_envelope_address(_channel, %Envelope{address: address}) when is_binary(address),
    do: {:ok, address}

  defp resolve_envelope_address(channel, %Envelope{params: params}),
    do: address_for(channel, params)

  defp publish_operations(router, table, operations, record, opts) do
    Enum.reduce_while(operations, {:ok, []}, fn operation, {:ok, published} ->
      case publish_operation(router, table, operation, record, opts) do
        {:ok, :filtered} -> {:cont, {:ok, published}}
        {:ok, envelope} -> {:cont, {:ok, [envelope | published]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, published} -> {:ok, Enum.reverse(published)}
      error -> error
    end
  end

  defp publish_operation(router, table, %ResolvedOperation{} = operation, record, opts) do
    channel = Table.channel(table, operation.channel_key)

    if filtered_out?(operation, record, opts) do
      {:ok, :filtered}
    else
      # Resolved once: a relationship-backed parameter can cost a query, and the envelope
      # needs the same values twice. The context carries what the record cannot: the
      # operation's event verb, for channels addressed with `:_event`.
      params =
        address_params(channel, record,
          domain: operation.domain,
          context: %{event: operation.event_verb}
        )

      with {:ok, payload} <- AshAsyncApi.Payload.for_send(operation, record),
           {:ok, address} <- address_for(channel, params) do
        envelope =
          Envelope.new(
            channel: channel.key,
            address: address,
            operation: operation.name,
            message: operation.message_name,
            payload: payload,
            headers: headers_for(operation, record),
            content_type: operation.content_type,
            resource: operation.resource,
            action: operation.action,
            params: params,
            reply_to: reply_address(table, operation, record),
            router: router
          )

        deliver(router, table, channel, envelope, opts)
      end
    end
  end

  defp filtered_out?(%{operation: %{filter: nil}}, _record, _opts), do: false

  defp filtered_out?(%{operation: %{filter: filter}}, record, opts) do
    context = opts[:notification] || Map.new(opts)

    not filter.(record, context)
  end

  defp headers_for(%{operation: %{headers: nil}}, _record), do: %{}

  defp headers_for(%{operation: %{headers: headers}}, _record) when is_map(headers) do
    Map.new(headers, fn {key, value} -> {to_string(key), value} end)
  end

  defp headers_for(%{operation: %{headers: headers}}, record) when is_function(headers, 1) do
    record |> headers.() |> Map.new(fn {key, value} -> {to_string(key), value} end)
  end

  defp reply_address(_table, %{reply_channel_key: nil}, _record), do: nil

  defp reply_address(table, %{reply_channel_key: key, domain: domain}, record) do
    channel = Table.channel(table, key)

    case address_for(channel, address_params(channel, record, domain: domain)) do
      {:ok, address} -> address
      _ -> nil
    end
  end

  @doc """
  The address parameter values a channel needs, read off a record.

  Each parameter carries the source path it was written with: `:id` reads the field, and
  `[:organization, :id]` walks the relationship. A `parameter` block's `source` overrides
  the path, and may be a function of the record.
  """
  @spec address_params(AshAsyncApi.Router.Table.ResolvedChannel.t(), term(), keyword()) :: map()
  def address_params(channel, record, opts \\ [])

  def address_params(%{compiled: nil}, _record, _opts), do: %{}

  def address_params(channel, record, opts) do
    paths = channel.compiled.param_paths

    Map.new(channel.compiled.params, fn name ->
      parameter = AshAsyncApi.Router.Table.ResolvedChannel.parameter(channel, name)
      path = Map.get(paths, name) || [name]

      {name, param_value(parameter, path, record, opts)}
    end)
  end

  defp param_value(nil, path, record, opts), do: resolve_path(path, record, opts)

  defp param_value(parameter, path, record, opts) do
    # A `parameter` block usually only documents; it overrides where the value comes from
    # only when it declares a `source`. Falling back to the parameter's *name* here would
    # silently break `[:organization, :id]`, whose value is not on the record at all.
    value =
      case AshAsyncApi.Channel.Parameter.source(parameter) do
        nil -> resolve_path(path, record, opts)
        source when is_function(source, 1) -> source.(record)
        source when is_list(source) -> walk(record, source, opts)
        source when is_atom(source) -> walk(record, [source], opts)
      end

    value || parameter.default
  end

  # `{:context, key}` values come from the publisher, not the record — the operation's
  # event verb today. `{:join, fields, joiner}` packs a composite primary key into one
  # address token; a nil component becomes `_` rather than sinking the whole message.
  defp resolve_path({:context, key}, _record, opts) do
    Map.get(opts[:context] || %{}, key)
  end

  defp resolve_path({:join, fields, joiner}, record, _opts) do
    Enum.map_join(fields, joiner, fn field ->
      case read_field(record, field) do
        nil -> "_"
        value -> to_string(value)
      end
    end)
  end

  # An unresolved special segment: only possible when a compiled address is used outside
  # the routing table. There is no record-side value for it.
  defp resolve_path({:special, _name}, _record, _opts), do: nil

  defp resolve_path(path, record, opts) when is_list(path), do: walk(record, path, opts)

  @doc false
  # Walk a source path to a value. `[:id]` is a plain field read; anything longer traverses
  # relationships.
  def walk(record, [field], _opts), do: read_field(record, field)

  def walk(record, [relationship | rest], opts) when is_map(record) do
    case fast_path(record, relationship, rest) do
      {:ok, value} -> value
      :error -> record |> load_related(relationship, rest, opts) |> walk(rest, opts)
    end
  end

  def walk(_record, _path, _opts), do: nil

  # A `belongs_to` already stores the key on the record, so `[:organization, :id]` needs no
  # load at all — which matters, because this runs in a notifier after every write.
  defp fast_path(%resource{} = record, relationship, [field]) do
    with true <- function_exported?(resource, :spark_dsl_config, 0),
         %Ash.Resource.Relationships.BelongsTo{} = rel <-
           Ash.Resource.Info.relationship(resource, relationship),
         true <- rel.destination_attribute == field do
      {:ok, read_field(record, rel.source_attribute)}
    else
      _ -> :error
    end
  end

  defp fast_path(_record, _relationship, _rest), do: :error

  defp load_related(record, relationship, rest, opts) do
    case Map.get(record, relationship) do
      %Ash.NotLoaded{} -> do_load(record, relationship, rest, opts)
      %Ash.ForbiddenField{} -> nil
      related -> related
    end
  end

  # Loading here is a query in the publishing path, so it is the fallback rather than the
  # rule — `fast_path/3` covers the common `belongs_to` case without touching the database.
  # `authorize?: false` because the decision to publish was already made; this is the system
  # reading its own record to address the message, not the actor reading data.
  defp do_load(%resource{} = record, relationship, rest, opts) do
    load = nested_load(relationship, rest)

    case Ash.load(record, load, domain: opts[:domain], authorize?: false) do
      {:ok, loaded} -> Map.get(loaded, relationship)
      {:error, reason} -> warn_unloadable(resource, relationship, reason)
    end
  rescue
    # Publishing runs in a notifier, after the caller's write has already committed. A
    # relationship that cannot be loaded — a nil foreign key, a resource Ash refuses to
    # query — must degrade to an unfillable address, never take down the action that
    # triggered it. `Ash.load` raises for some of these rather than returning an error.
    error -> warn_unloadable(resource, relationship, error)
  end

  defp warn_unloadable(resource, relationship, reason) do
    Logger.warning("""
    AshAsyncApi could not load #{inspect(relationship)} on #{inspect(resource)} to build a \
    channel address: #{format_error(reason)}

    Preload it before publishing, or address the channel with a field that is already \
    present — a belongs_to's own key, for instance.
    """)

    nil
  end

  defp format_error(%{__exception__: true} = error), do: Exception.message(error)
  defp format_error(reason), do: inspect(reason)

  defp nested_load(relationship, [_field]), do: [relationship]
  defp nested_load(relationship, [next | rest]), do: [{relationship, nested_load(next, rest)}]

  defp read_field(record, name) when is_map(record) do
    case Map.get(record, name) do
      %Ash.NotLoaded{} -> nil
      %Ash.ForbiddenField{} -> nil
      value -> value
    end
  end

  defp read_field(_record, _name), do: nil

  defp address_for(%{address: nil}, _params), do: {:ok, nil}

  defp address_for(channel, params) do
    case AshAsyncApi.Address.interpolate(channel.compiled, params) do
      {:ok, address} ->
        {:ok, address}

      {:error, {:missing_params, missing}} ->
        {:error,
         AshAsyncApi.Error.MissingAddressParams.exception(
           address: channel.address,
           missing: missing,
           channel: channel.name,
           paths: channel.compiled.param_paths
         )}
    end
  end

  defp deliver(router, table, channel, envelope, opts) do
    envelope = Envelope.put_header(envelope, @origin_header, origin(router))

    if Keyword.get(opts, :broadcast?, true) do
      AshAsyncApi.PubSub.broadcast(router, envelope)
    end

    case Keyword.get(opts, :transports?, true) && to_transports(table, channel, envelope) do
      false -> {:ok, envelope}
      :ok -> {:ok, envelope}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_transports(table, channel, envelope) do
    active = AshAsyncApi.Supervisor.active_servers(table.router)
    contexts = Enum.flat_map(channel.servers, &server_context(table, &1, active))

    # A channel with no transport at all is a deliberate configuration — spec-only,
    # or in-cluster delivery over `AshAsyncApi.PubSub`. Nothing to do, and nothing wrong.
    case contexts do
      [] -> :ok
      contexts -> publish_to_contexts(contexts, envelope)
    end
  end

  # A server the supervisor deliberately did not start — disabled at runtime, or
  # `start_transports?: false` — is a silent skip, not a failure. When no supervisor has
  # run at all (`active` is nil) every server is assumed live, so a missing connection
  # still surfaces as the error it is.
  defp server_context(table, name, active) do
    with {domain, %{transport: transport} = server} when not is_nil(transport) <-
           Map.get(table.servers, name),
         true <- is_nil(active) or MapSet.member?(active, name) do
      [Context.new(table.router, domain, server)]
    else
      _ -> []
    end
  end

  defp publish_to_contexts(contexts, envelope) do
    results =
      Enum.map(contexts, fn context ->
        {context, publish_one(context, envelope)}
      end)

    case Enum.filter(results, &match?({_context, {:error, _}}, &1)) do
      [] ->
        :ok

      failures ->
        for {context, {:error, reason}} <- failures do
          Logger.error("""
          AshAsyncApi failed to publish to #{inspect(context.server.name)} \
          (#{inspect(context.transport)}) at #{inspect(envelope.address)}: #{inspect(reason)}
          """)
        end

        # Succeeding on one broker while failing on another is still a failure — the
        # caller decides whether that is fatal.
        {:error,
         {:publish_failed,
          Enum.map(failures, fn {context, error} -> {context.server.name, error} end)}}
    end
  end

  defp publish_one(context, envelope) do
    with {:ok, body} <- context.transport.encode(context, envelope) do
      context.transport.publish(context, envelope.address, body, publish_opts(context, envelope))
    end
  end

  defp publish_opts(_context, envelope) do
    [
      content_type: envelope.content_type,
      correlation_id: envelope.correlation_id,
      reply_to: envelope.reply_to,
      headers: envelope.headers
    ]
  end

  defp unknown_operation_error(router, table, resource, action) do
    known =
      table.operations
      |> Enum.filter(&(&1.resource == resource))
      |> Enum.map(&{&1.direction, &1.action, &1.channel_key})

    AshAsyncApi.Error.UnknownOperation.exception(
      resource: resource,
      action: action,
      direction: :send,
      router: router,
      known: known
    )
  end

  @doc """
  The origin marker stamped on outbound messages: the router and the node that sent it.
  """
  @spec origin(module()) :: String.t()
  def origin(router), do: "#{inspect(router)}@#{node()}"

  @doc """
  Whether an inbound envelope was published by this router, from a node in this cluster.

  Both halves matter — see the "Loops" section of `AshAsyncApi.Router`.
  """
  @spec own_message?(module(), Envelope.t()) :: boolean()
  def own_message?(router, %Envelope{} = envelope) do
    case Envelope.get_header(envelope, @origin_header) do
      nil ->
        false

      origin ->
        case String.split(origin, "@", parts: 2) do
          [origin_router, origin_node] ->
            origin_router == inspect(router) and
              origin_node in Enum.map([node() | Node.list()], &Atom.to_string/1)

          _ ->
            false
        end
    end
  end
end
