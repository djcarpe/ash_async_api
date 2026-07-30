defmodule AshAsyncApi.Transport.Kafka.Connection do
  @moduledoc """
  Owns the `brod` client and consumer group for one Kafka server.

  Consuming uses `:brod_group_subscriber_v2`, so partition assignment and rebalancing
  are Kafka's problem rather than ours. Offsets are committed after
  `AshAsyncApi.Transport.deliver/4` returns, which gives at-least-once processing: a
  crash mid-message means the message is redelivered, so `subscribe` operations on a
  Kafka channel should be idempotent (`upsert? true` on creates is usually the answer).
  """

  use GenServer

  require Logger

  alias AshAsyncApi.Transport.Context
  alias AshAsyncApi.Transport.Kafka

  # `:brod` is an optional dependency; see the moduledoc of `AshAsyncApi.Transport.Kafka`.
  @compile {:no_warn_undefined, [:brod, :brod_group_subscriber_v2]}

  defstruct [:context, :client, :topics, :subscribers]

  @doc false
  def start_link(%Context{} = context) do
    GenServer.start_link(__MODULE__, context, name: Context.process_name(context))
  end

  @doc """
  Publish to the topic derived from `address`, keyed by the address's parameters.
  """
  def publish(%Context{} = context, address, body, opts) do
    template = opts[:address_template] || address
    {topic, key} = Kafka.split_address(address, template)
    client = client_id(context)

    :brod.produce_sync(
      client,
      topic,
      partition_fun(context, key),
      key || "",
      IO.iodata_to_binary(body)
    )
  catch
    :exit, reason -> {:error, reason}
  end

  @doc "Start consuming a topic."
  def subscribe(%Context{} = context, topic) do
    GenServer.call(Context.process_name(context), {:subscribe, topic}, 30_000)
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
  end

  @impl true
  def init(%Context{} = context) do
    Process.flag(:trap_exit, true)

    topics =
      if Context.opt(context, :auto_subscribe?, true) do
        Context.opt(context, :filters, [])
      else
        []
      end

    {:ok, %__MODULE__{context: context, topics: topics, subscribers: %{}}, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case start_client(state.context) do
      :ok ->
        state = Enum.reduce(state.topics, state, &start_subscriber(&2, &1))
        {:noreply, state}

      {:error, reason} ->
        Logger.error("""
        AshAsyncApi could not start the Kafka client for \
        #{inspect(state.context.server.name)}: #{inspect(reason)}
        """)

        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call({:subscribe, topic}, _from, state) do
    if Map.has_key?(state.subscribers, topic) do
      {:reply, :ok, state}
    else
      state = start_subscriber(state, topic)

      case Map.get(state.subscribers, topic) do
        nil -> {:reply, {:error, :subscribe_failed}, state}
        _pid -> {:reply, :ok, state}
      end
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case Enum.find(state.subscribers, fn {_topic, subscriber} -> subscriber == pid end) do
      nil ->
        {:noreply, state}

      {topic, _pid} ->
        Logger.warning(
          "AshAsyncApi Kafka subscriber for #{topic} exited: #{inspect(reason)}; restarting"
        )

        {:noreply,
         start_subscriber(%{state | subscribers: Map.delete(state.subscribers, topic)}, topic)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :brod.stop_client(client_id(state.context))
    :ok
  catch
    _, _ -> :ok
  end

  defp start_client(context) do
    client = client_id(context)
    endpoints = endpoints(context)
    config = Context.opt(context, :client_config, [])

    case :brod.start_client(endpoints, client, config) do
      :ok -> :brod.start_producer(client, :undefined, Context.opt(context, :producer_config, []))
      {:error, {:already_started, _}} -> :ok
      error -> error
    end
  end

  defp start_subscriber(state, topic) do
    context = state.context

    config = %{
      client: client_id(context),
      group_id: to_string(Context.opt!(context, :group_id)),
      topics: [topic],
      cb_module: AshAsyncApi.Transport.Kafka.Subscriber,
      group_config: Context.opt(context, :group_config, []),
      consumer_config:
        Keyword.put_new(Context.opt(context, :consumer_config, []), :begin_offset, :latest),
      init_data: %{context: context, topic: topic}
    }

    case :brod_group_subscriber_v2.start_link(config) do
      {:ok, pid} ->
        Logger.debug("AshAsyncApi Kafka consuming #{topic}")
        %{state | subscribers: Map.put(state.subscribers, topic, pid)}

      {:error, reason} ->
        Logger.error("AshAsyncApi Kafka could not consume #{topic}: #{inspect(reason)}")
        state
    end
  end

  defp endpoints(context) do
    case Context.opt(context, :endpoints) do
      nil -> [host_endpoint(context)]
      endpoints -> endpoints
    end
  end

  defp host_endpoint(context) do
    case String.split(context.server.host, ":", parts: 2) do
      [host, port] ->
        case Integer.parse(port) do
          {port, ""} -> {to_charlist(host), port}
          _ -> {to_charlist(host), 9092}
        end

      [host] ->
        {to_charlist(host), 9092}
    end
  end

  # Hashing the key is what keeps a given entity's events in one partition, in order.
  defp partition_fun(context, key) do
    case Context.opt(context, :partitioner, :hash) do
      :hash when is_binary(key) -> hash_partitioner(key)
      :hash -> :random
      :random -> :random
      fun when is_function(fun) -> fun
      other -> other
    end
  end

  defp hash_partitioner(key) do
    fn _topic, partition_count, _key, _value ->
      {:ok, :erlang.phash2(key, partition_count)}
    end
  end

  @doc false
  def client_id(%Context{} = context) do
    Context.opt(context, :client_id) ||
      Module.concat(Context.process_name(context), "Client")
  end
end
