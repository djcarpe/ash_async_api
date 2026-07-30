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
      with {:ok, payload} <- AshAsyncApi.Payload.for_send(operation, record),
           {:ok, address} <- address_for(channel, address_params(channel, record)) do
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
            params: address_params(channel, record),
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

  defp reply_address(table, %{reply_channel_key: key}, record) do
    channel = Table.channel(table, key)

    case address_for(channel, address_params(channel, record)) do
      {:ok, address} -> address
      _ -> nil
    end
  end

  @doc """
  The address parameter values a channel needs, read off a record.

  Every `{parameter}` in the address needs a value. Unless the channel's `parameter`
  block says otherwise, it comes from the field of the same name on the record.
  """
  @spec address_params(AshAsyncApi.Router.Table.ResolvedChannel.t(), term()) :: map()
  def address_params(%{compiled: nil}, _record), do: %{}

  def address_params(channel, record) do
    Map.new(channel.compiled.params, fn name ->
      parameter = AshAsyncApi.Router.Table.ResolvedChannel.parameter(channel, name)
      {name, param_value(parameter, name, record)}
    end)
  end

  defp param_value(nil, name, record), do: read_field(record, name)

  defp param_value(parameter, name, record) do
    case AshAsyncApi.Channel.Parameter.source(parameter) do
      source when is_function(source, 1) -> source.(record)
      source when is_atom(source) -> read_field(record, source) || parameter.default
      _ -> read_field(record, name) || parameter.default
    end
  end

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
           channel: channel.name
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
    contexts =
      channel.servers
      |> Enum.flat_map(fn name ->
        case Map.get(table.servers, name) do
          {domain, %{transport: transport} = server} when not is_nil(transport) ->
            [Context.new(table.router, domain, server)]

          _ ->
            []
        end
      end)

    # A channel with no transport at all is a deliberate configuration — spec-only,
    # or in-cluster delivery over `AshAsyncApi.PubSub`. Nothing to do, and nothing wrong.
    case contexts do
      [] -> :ok
      contexts -> publish_to_contexts(contexts, envelope)
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
