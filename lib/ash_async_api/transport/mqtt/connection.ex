defmodule AshAsyncApi.Transport.Mqtt.Connection do
  @moduledoc """
  Holds the `emqtt` connection for one MQTT server and pumps received publishes into
  `AshAsyncApi.Transport.deliver/4`.

  Reconnection is handled by reconnecting rather than by crashing: an unreachable
  broker is an expected condition, not a bug, and letting the supervisor restart-loop
  on it would take the whole router down. Publishing while disconnected returns
  `{:error, :disconnected}` so the caller can decide what to do.

  Subscriptions are re-established after every reconnect, since MQTT sessions with
  `clean_start: true` forget them.
  """

  use GenServer

  require Logger

  alias AshAsyncApi.Transport.Context

  # `:emqtt` is an optional dependency; see the moduledoc of `AshAsyncApi.Transport.Mqtt`.
  @compile {:no_warn_undefined, :emqtt}

  @default_qos 1
  @default_reconnect_interval 5_000

  defstruct [:context, :client, :filters, :monitor, connected?: false]

  @doc false
  def start_link(%Context{} = context) do
    GenServer.start_link(__MODULE__, context, name: Context.process_name(context))
  end

  @doc """
  Publish to a topic. Returns `{:error, :disconnected}` when the broker is unreachable.
  """
  def publish(%Context{} = context, topic, body, opts) do
    GenServer.call(Context.process_name(context), {:publish, topic, body, opts})
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  @doc "Subscribe to a topic filter."
  def subscribe(%Context{} = context, filter) do
    GenServer.call(Context.process_name(context), {:subscribe, filter})
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
  end

  @doc "Unsubscribe from a topic filter."
  def unsubscribe(%Context{} = context, filter) do
    GenServer.call(Context.process_name(context), {:unsubscribe, filter})
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
  end

  @impl true
  def init(%Context{} = context) do
    Process.flag(:trap_exit, true)

    filters =
      if Context.opt(context, :auto_subscribe?, true) do
        Context.opt(context, :filters, [])
      else
        []
      end

    {:ok, %__MODULE__{context: context, filters: filters}, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    {:noreply, connect(state)}
  end

  @impl true
  def handle_call({:publish, _topic, _body, _opts}, _from, %{connected?: false} = state) do
    {:reply, {:error, :disconnected}, state}
  end

  def handle_call({:publish, topic, body, opts}, _from, state) do
    properties = properties(opts)

    result =
      :emqtt.publish(
        state.client,
        topic,
        properties,
        IO.iodata_to_binary(body),
        qos: qos(state.context, opts),
        retain: retain(state.context, opts)
      )

    {:reply, normalize(result), state}
  end

  def handle_call({:subscribe, filter}, _from, state) do
    state = %{state | filters: Enum.uniq([filter | state.filters])}

    if state.connected? do
      {:reply, do_subscribe(state, filter), state}
    else
      # Recorded now, applied on connect.
      {:reply, :ok, state}
    end
  end

  def handle_call({:unsubscribe, filter}, _from, state) do
    state = %{state | filters: List.delete(state.filters, filter)}

    if state.connected? do
      {:reply, normalize(:emqtt.unsubscribe(state.client, filter)), state}
    else
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:publish, publish}, state) do
    deliver(state, publish)
    {:noreply, state}
  end

  def handle_info({:disconnected, reason, _properties}, state) do
    Logger.warning(
      "AshAsyncApi MQTT connection to #{inspect(server_name(state))} lost: #{inspect(reason)}"
    )

    {:noreply, schedule_reconnect(%{state | connected?: false})}
  end

  def handle_info(:reconnect, state) do
    {:noreply, connect(state)}
  end

  def handle_info({:EXIT, client, reason}, %{client: client} = state) do
    Logger.warning(
      "AshAsyncApi MQTT client for #{inspect(server_name(state))} exited: #{inspect(reason)}"
    )

    {:noreply, schedule_reconnect(%{state | connected?: false, client: nil})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{client: client}) when is_pid(client) do
    :emqtt.disconnect(client)
    :ok
  catch
    _, _ -> :ok
  end

  def terminate(_reason, _state), do: :ok

  defp connect(state) do
    with {:ok, client} <- start_client(state.context),
         {:ok, _properties} <- :emqtt.connect(client) do
      Logger.info("AshAsyncApi MQTT connected to #{inspect(server_name(state))}")

      state = %{state | client: client, connected?: true}
      Enum.each(state.filters, &do_subscribe(state, &1))
      state
    else
      {:error, reason} ->
        Logger.warning("""
        AshAsyncApi could not connect to MQTT server #{inspect(server_name(state))} \
        at #{state.context.server.host}: #{inspect(reason)}
        """)

        schedule_reconnect(%{state | connected?: false})
    end
  end

  defp start_client(context) do
    {host, port} = host_and_port(context)

    opts =
      context.opts
      |> Keyword.drop([
        :qos,
        :retain,
        :subscribe_qos,
        :reconnect_interval,
        :auto_subscribe?,
        :filters
      ])
      |> Enum.map(&resolve_system_opt/1)
      |> Keyword.put_new(:host, host)
      |> Keyword.put_new(:port, port)
      |> Keyword.put(:owner, self())

    :emqtt.start_link(Map.new(opts))
  end

  # The AsyncAPI server `host` may carry the port, since that is how the spec models it.
  defp host_and_port(context) do
    case String.split(context.server.host, ":", parts: 2) do
      [host, port] ->
        case Integer.parse(port) do
          {port, ""} -> {to_charlist(host), port}
          _ -> {to_charlist(host), default_port(context)}
        end

      [host] ->
        {to_charlist(host), default_port(context)}
    end
  end

  defp default_port(%{server: %{protocol: :mqtts}}), do: 8883
  defp default_port(_context), do: 1883

  defp resolve_system_opt({key, {:system, variable}}) do
    {key, System.get_env(variable)}
  end

  defp resolve_system_opt({key, {:system, variable, default}}) do
    {key, System.get_env(variable, default)}
  end

  defp resolve_system_opt(other), do: other

  defp do_subscribe(state, filter) do
    qos = Context.opt(state.context, :subscribe_qos) || qos(state.context, [])

    case :emqtt.subscribe(state.client, {filter, qos}) do
      {:ok, _properties, _reason_codes} ->
        Logger.debug("AshAsyncApi MQTT subscribed to #{filter}")
        :ok

      other ->
        Logger.error("AshAsyncApi MQTT could not subscribe to #{filter}: #{inspect(other)}")
        normalize(other)
    end
  end

  defp deliver(state, publish) do
    topic = publish |> Map.get(:topic) |> to_string()
    payload = Map.get(publish, :payload)
    properties = Map.get(publish, :properties) || %{}

    AshAsyncApi.Transport.deliver(state.context, topic, payload,
      headers: user_properties(properties),
      content_type: properties[:"Content-Type"],
      correlation_id: properties[:"Correlation-Data"],
      reply_to: properties[:"Response-Topic"],
      metadata: %{qos: Map.get(publish, :qos), retain: Map.get(publish, :retain)}
    )
  rescue
    error ->
      Logger.error("""
      AshAsyncApi failed to handle an MQTT message on #{inspect(Map.get(publish, :topic))}:
      #{Exception.format(:error, error, __STACKTRACE__)}
      """)
  end

  # MQTT 5 user properties are a list of pairs, which is the closest thing MQTT has
  # to headers.
  defp user_properties(%{"User-Property": pairs}) when is_list(pairs) do
    Map.new(pairs, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp user_properties(_properties), do: %{}

  defp properties(opts) do
    %{}
    |> put_property("Content-Type", opts[:content_type])
    |> put_property("Correlation-Data", opts[:correlation_id])
    |> put_property("Response-Topic", opts[:reply_to])
  end

  defp put_property(properties, _key, nil), do: properties
  defp put_property(properties, key, value), do: Map.put(properties, :"#{key}", value)

  defp qos(context, opts) do
    binding(opts, :qos) || Context.opt(context, :qos, @default_qos)
  end

  defp retain(context, opts) do
    case binding(opts, :retain) do
      nil -> Context.opt(context, :retain, false)
      retain -> retain
    end
  end

  defp binding(opts, key) do
    opts
    |> Keyword.get(:bindings, %{})
    |> Map.get(:mqtt, %{})
    |> Map.get(key)
  end

  defp schedule_reconnect(state) do
    interval = Context.opt(state.context, :reconnect_interval, @default_reconnect_interval)
    Process.send_after(self(), :reconnect, interval)
    state
  end

  defp server_name(%{context: %{server: %{name: name}}}), do: name

  defp normalize(:ok), do: :ok
  defp normalize({:ok, _}), do: :ok
  defp normalize({:ok, _, _}), do: :ok
  defp normalize({:error, reason}), do: {:error, reason}
  defp normalize(other), do: {:error, other}
end
