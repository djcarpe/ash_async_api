defmodule AshAsyncApi.Router.Inbound do
  @moduledoc """
  The inbound path: bytes off the wire, actions run, subscribers notified.

  Transports call `AshAsyncApi.Transport.deliver/4`, which lands here. What happens
  then:

    1. Decode the body and match the concrete address against the router's channels,
       extracting any address parameters.
    2. Drop the message if this router published it (see the "Loops" section of
       `AshAsyncApi.Router`).
    3. Run each `subscribe` operation on the matched channel, *on this node only* —
       the node that received the message from the broker. Running the action once is
       the whole point; fanning it out would run it once per node.
    4. Fan the envelope out to subscribed processes across the cluster via
       `AshAsyncApi.PubSub`.

  Steps 3 and 4 are deliberately different: actions have effects and must happen
  exactly once, while subscribers are observers and all of them should see it.
  """

  require Logger

  alias AshAsyncApi.Envelope
  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Router.Table.ResolvedOperation
  alias AshAsyncApi.Transport.Context

  @doc """
  Handle a message received by a transport. See `AshAsyncApi.Transport.deliver/4`.
  """
  @spec deliver(Context.t(), String.t(), binary(), keyword()) ::
          {:ok, [Envelope.t()]} | {:error, term()}
  def deliver(%Context{} = context, address, body, opts \\ []) do
    router = context.router
    table = router.__ash_async_api__()

    case Table.match_address(table, address, context.server.name) do
      [] ->
        no_route(table, address, context, opts)

      matches ->
        with {:ok, decoded} <- context.transport.decode(context, body) do
          envelopes =
            Enum.flat_map(matches, fn {channel, params} ->
              handle_match(context, table, channel, params, address, decoded, opts)
            end)

          {:ok, envelopes}
        end
    end
  end

  defp handle_match(context, table, channel, params, address, decoded, opts) do
    envelope = build_envelope(context, channel, params, address, decoded, opts)

    if ignore?(context, envelope, opts) do
      Logger.debug(fn ->
        "AshAsyncApi ignored a message at #{address} published by this router"
      end)

      []
    else
      run_operations(context, table, channel, envelope)
      broadcast(context, envelope, opts)
      [envelope]
    end
  end

  defp build_envelope(context, channel, params, address, decoded, opts) do
    Envelope.from_wire(decoded,
      channel: channel.key,
      address: address,
      params: params,
      server: context.server.name,
      router: context.router,
      content_type:
        opts[:content_type] || AshAsyncApi.Domain.Info.default_content_type(channel.domain),
      correlation_id: opts[:correlation_id],
      reply_to: opts[:reply_to]
    )
    |> merge_transport_headers(opts)
    |> Map.put(:metadata, Map.new(opts[:metadata] || %{}))
  end

  # Headers the broker delivered sit alongside headers that travelled inside the
  # envelope; the envelope's own win, since they are what the sender meant.
  defp merge_transport_headers(envelope, opts) do
    case opts[:headers] do
      nil ->
        envelope

      headers ->
        headers = Map.new(headers, fn {key, value} -> {to_string(key), value} end)
        %{envelope | headers: Map.merge(headers, envelope.headers)}
    end
  end

  defp ignore?(context, envelope, opts) do
    cond do
      Keyword.get(opts, :skip_origin_check?, false) -> false
      not AshAsyncApi.Router.config(context.router).ignore_own_messages? -> false
      true -> AshAsyncApi.Publisher.own_message?(context.router, envelope)
    end
  end

  defp broadcast(context, envelope, opts) do
    if Keyword.get(opts, :broadcast?, true) do
      case AshAsyncApi.Transport.delivery_scope(context.transport) do
        :local -> AshAsyncApi.PubSub.broadcast_local(context.router, envelope)
        :cluster -> AshAsyncApi.PubSub.broadcast(context.router, envelope)
      end
    end

    :ok
  end

  defp run_operations(context, table, channel, envelope) do
    channel
    |> AshAsyncApi.Router.Table.ResolvedChannel.operations(:receive)
    |> Enum.filter(&matches_message?(&1, envelope))
    |> Enum.each(&run_operation(context, table, &1, envelope))
  end

  # A channel can carry several message types. When the sender named the message, only
  # the operation expecting that name runs; when it did not, every operation on the
  # channel gets a look, which is what lets AshAsyncApi consume foreign topics.
  defp matches_message?(_operation, %Envelope{message: nil}), do: true

  defp matches_message?(%ResolvedOperation{message_name: message_name}, %Envelope{
         message: message
       }) do
    message_name == message
  end

  defp run_operation(context, table, operation, envelope) do
    if filtered_out?(operation, envelope) do
      :ok
    else
      case AshAsyncApi.Payload.for_receive(operation, envelope.payload, envelope) do
        {:ok, input} -> run_action(context, table, operation, envelope, input)
        {:error, reason} -> log_invalid_payload(operation, envelope, reason)
      end
    end
  end

  defp filtered_out?(%{operation: %{filter: nil}}, _envelope), do: false

  defp filtered_out?(%{operation: %{filter: filter}}, envelope) do
    not filter.(envelope.payload, envelope)
  end

  defp run_action(context, table, operation, envelope, input) do
    metadata = %{
      router: context.router,
      resource: operation.resource,
      action: operation.action,
      operation: operation.name,
      channel: operation.channel_key,
      address: envelope.address
    }

    :telemetry.span([:ash_async_api, :receive], metadata, fn ->
      result = do_run_action(table, operation, envelope, input)
      {result, Map.put(metadata, :result, elem_tag(result))}
    end)
    |> case do
      {:ok, result} ->
        maybe_reply(context, table, operation, envelope, result)

      {:error, reason} ->
        Logger.error("""
        AshAsyncApi failed to run #{inspect(operation.resource)}.#{operation.action} \
        for a message received at #{inspect(envelope.address)}:

        #{format_error(reason)}
        """)

        {:error, reason}
    end
  end

  defp do_run_action(_table, operation, envelope, input) do
    action = Ash.Resource.Info.action(operation.resource, operation.action)
    opts = action_opts(operation, envelope)

    case action.type do
      :create ->
        operation.resource
        |> Ash.Changeset.for_create(operation.action, input, opts)
        |> then(&Ash.create(&1, create_opts(operation)))

      :read ->
        operation.resource
        |> Ash.Query.for_read(operation.action, input, opts)
        |> Ash.read()

      :action ->
        operation.resource
        |> Ash.ActionInput.for_action(operation.action, input, opts)
        |> Ash.run_action()

      type when type in [:update, :destroy] ->
        run_on_record(operation, action, input, opts, type)
    end
  end

  # Update and destroy need a record to act on. The primary key comes from the
  # payload or, more often, from the address — `tickets/42/commands` says which
  # ticket without the payload having to repeat it.
  defp run_on_record(operation, action, input, opts, type) do
    primary_key = Ash.Resource.Info.primary_key(operation.resource)
    {key_values, attributes} = Map.split(input, primary_key)

    if map_size(key_values) < length(primary_key) do
      {:error,
       AshAsyncApi.Error.InvalidPayload.exception(
         reason: """
         Cannot #{type} without the primary key. Expected #{inspect(primary_key)} in the \
         message payload or in the channel address, got #{inspect(Map.keys(input))}.
         """,
         operation: operation.name,
         address: nil
       )}
    else
      operation.resource
      |> Ash.get(key_values, Keyword.take(opts, [:actor, :tenant, :authorize?]))
      |> case do
        {:ok, record} -> apply_to_record(record, action, attributes, opts, type)
        error -> error
      end
    end
  end

  defp apply_to_record(record, action, attributes, opts, :update) do
    record
    |> Ash.Changeset.for_update(action.name, attributes, opts)
    |> Ash.update()
  end

  defp apply_to_record(record, action, attributes, opts, :destroy) do
    record
    |> Ash.Changeset.for_destroy(action.name, attributes, opts)
    |> Ash.destroy()
  end

  defp create_opts(%{operation: %{upsert?: true}}), do: [upsert?: true]
  defp create_opts(_operation), do: []

  defp action_opts(operation, envelope) do
    [
      actor: resolve(operation.operation.actor, envelope),
      tenant: resolve(operation.operation.tenant, envelope),
      domain: operation.domain,
      context: %{ash_async_api: %{envelope: envelope}}
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp resolve(nil, _envelope), do: nil
  defp resolve(value, envelope) when is_function(value, 1), do: value.(envelope)
  defp resolve(value, _envelope), do: value

  defp maybe_reply(_context, _table, %{reply_channel_key: nil}, _envelope, _result), do: :ok

  defp maybe_reply(context, table, operation, envelope, result) do
    channel = Table.channel(table, operation.reply_channel_key)

    payload = reply_payload(operation, result)

    AshAsyncApi.Publisher.publish_to(context.router, channel.key, payload,
      params: reply_params(channel, envelope, result),
      correlation_id: envelope.correlation_id || envelope.id,
      message: operation.message_name && "#{operation.message_name}Reply"
    )
    |> case do
      {:ok, _envelope} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "AshAsyncApi could not publish a reply on #{inspect(channel.key)}: #{inspect(reason)}"
        )

        :ok
    end
  end

  # The reply channel's address is usually about the *result* — a command arriving on
  # `tickets/commands` replies on `tickets/<new id>/events`, and the new id exists only
  # on the record the action returned. The inbound envelope's own params fill in
  # anything the record cannot supply.
  defp reply_params(channel, envelope, result) do
    from_result =
      case result do
        %_struct{} = record -> AshAsyncApi.Publisher.address_params(channel, record)
        _ -> %{}
      end

    from_result
    |> Enum.reject(fn {_name, value} -> is_nil(value) end)
    |> Map.new()
    |> then(&Map.merge(envelope.params, &1))
  end

  defp reply_payload(operation, result) when is_list(result) do
    Enum.map(result, &reply_payload(operation, &1))
  end

  defp reply_payload(operation, %resource{} = record) when is_atom(resource) do
    case AshAsyncApi.Payload.for_send(operation, record) do
      {:ok, payload} -> payload
      _ -> record
    end
  end

  defp reply_payload(_operation, result), do: result

  defp no_route(table, address, context, opts) do
    if Keyword.get(opts, :error_on_no_route?, false) do
      {:error,
       AshAsyncApi.Error.NoRoute.exception(
         address: address,
         server: context.server.name,
         router: context.router,
         known: Enum.map(table.inbound, & &1.address)
       )}
    else
      Logger.debug(fn ->
        "AshAsyncApi received a message at #{address} matching no channel; ignoring"
      end)

      {:ok, []}
    end
  end

  defp log_invalid_payload(operation, envelope, reason) do
    Logger.error(
      Exception.message(
        AshAsyncApi.Error.InvalidPayload.exception(
          reason: reason,
          payload: envelope.payload,
          operation: operation.name,
          address: envelope.address
        )
      )
    )

    {:error, reason}
  end

  defp elem_tag({:ok, _}), do: :ok
  defp elem_tag(:ok), do: :ok
  defp elem_tag(_), do: :error

  defp format_error(%{__exception__: true} = error), do: Exception.message(error)
  defp format_error(error), do: inspect(error)
end
