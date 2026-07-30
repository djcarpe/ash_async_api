defmodule AshAsyncApi.Transport.Nats.Connection do
  @moduledoc """
  Supervises a `Gnat` connection for one NATS server and routes received messages into
  `AshAsyncApi.Transport.deliver/4`.

  `Gnat.ConnectionSupervisor` owns the socket and handles reconnection, so this module owns
  only the subscriptions. It cannot subscribe eagerly, for two reasons that amount to the
  same thing: the connection supervisor connects *asynchronously*, so the named `Gnat`
  process does not exist yet when `start_link/1` returns, and after a reconnect the old
  subscriptions are gone.

  So subscriptions are **reconciled** rather than registered. Every tick, this process
  compares the `Gnat` pid it last subscribed through against the one currently registered:
  if it changed (including from `nil` at startup), every filter is re-subscribed. That
  handles the startup race and reconnection with one mechanism and no reliance on
  connection notifications.
  """

  use GenServer

  require Logger

  alias AshAsyncApi.Transport.Context

  # `:gnat` is an optional dependency — only applications that actually use NATS need
  # to pull it in, so it is legitimately absent when this module compiles.
  @compile {:no_warn_undefined, [Gnat, Gnat.ConnectionSupervisor]}

  @default_reply_timeout 5_000
  @reconcile_interval 1_000

  defstruct [:context, :gnat_name, :subscriptions, :subscribed_through]

  @doc false
  def start_link(%Context{} = context) do
    GenServer.start_link(__MODULE__, context, name: Context.process_name(context))
  end

  @doc "Publish to a subject."
  def publish(%Context{} = context, subject, body, opts) do
    case Process.whereis(gnat_name(context)) do
      nil -> {:error, :disconnected}
      pid -> Gnat.pub(pid, subject, IO.iodata_to_binary(body), pub_opts(opts))
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @doc "Subscribe to a subject filter."
  def subscribe(%Context{} = context, filter) do
    GenServer.call(Context.process_name(context), {:subscribe, filter})
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
  end

  @doc "Unsubscribe from a subject filter."
  def unsubscribe(%Context{} = context, filter) do
    GenServer.call(Context.process_name(context), {:unsubscribe, filter})
  catch
    :exit, {:noproc, _} -> {:error, :not_started}
  end

  @doc """
  Send a request and wait for a reply, using NATS' built-in request/reply.
  """
  def request(%Context{} = context, subject, body, opts \\ []) do
    timeout =
      Keyword.get(opts, :timeout, Context.opt(context, :reply_timeout, @default_reply_timeout))

    case Process.whereis(gnat_name(context)) do
      nil -> {:error, :disconnected}
      pid -> Gnat.request(pid, subject, IO.iodata_to_binary(body), receive_timeout: timeout)
    end
  end

  @impl true
  def init(%Context{} = context) do
    filters =
      if Context.opt(context, :auto_subscribe?, true) do
        Context.opt(context, :filters, [])
      else
        []
      end

    state = %__MODULE__{
      context: context,
      gnat_name: gnat_name(context),
      subscriptions: Map.new(filters, &{&1, nil}),
      subscribed_through: nil
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case start_connection(state) do
      :ok ->
        schedule_reconcile()
        {:noreply, state}

      {:error, reason} ->
        Logger.error("""
        AshAsyncApi could not start the NATS connection for \
        #{inspect(state.context.server.name)}: #{inspect(reason)}
        """)

        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call({:subscribe, filter}, _from, state) do
    # Recorded either way. If the connection is not up yet, the next reconcile registers it.
    state = put_in(state.subscriptions[filter], nil)

    case Process.whereis(state.gnat_name) do
      nil ->
        {:reply, :ok, state}

      pid ->
        case do_subscribe(state, pid, filter) do
          {:ok, sid} -> {:reply, :ok, put_in(state.subscriptions[filter], sid)}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:unsubscribe, filter}, _from, state) do
    {sid, subscriptions} = Map.pop(state.subscriptions, filter)
    pid = Process.whereis(state.gnat_name)

    if sid && pid, do: safely(fn -> Gnat.unsub(pid, sid) end)

    {:reply, :ok, %{state | subscriptions: subscriptions}}
  end

  @impl true
  def handle_info(:reconcile, state) do
    schedule_reconcile()
    {:noreply, reconcile(state)}
  end

  def handle_info({:msg, message}, state) do
    deliver(state, message)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # The whole of the connection lifecycle handling: if the Gnat process we subscribed
  # through is not the one registered now, our subscriptions are stale.
  defp reconcile(%{subscriptions: subscriptions} = state) when map_size(subscriptions) == 0 do
    state
  end

  defp reconcile(state) do
    case Process.whereis(state.gnat_name) do
      nil ->
        %{state | subscribed_through: nil}

      pid when pid == state.subscribed_through ->
        state

      pid ->
        Logger.debug(
          "AshAsyncApi NATS (re)subscribing #{map_size(state.subscriptions)} filter(s)"
        )

        subscriptions =
          Map.new(state.subscriptions, fn {filter, _sid} ->
            case do_subscribe(state, pid, filter) do
              {:ok, sid} -> {filter, sid}
              {:error, _reason} -> {filter, nil}
            end
          end)

        # Only record the pid once every filter took, so a partial failure is retried.
        subscribed_through =
          if Enum.any?(subscriptions, fn {_filter, sid} -> is_nil(sid) end), do: nil, else: pid

        %{state | subscriptions: subscriptions, subscribed_through: subscribed_through}
    end
  end

  defp schedule_reconcile, do: Process.send_after(self(), :reconcile, @reconcile_interval)

  defp start_connection(state) do
    child = %{
      id: {Gnat.ConnectionSupervisor, state.gnat_name},
      start:
        {Gnat.ConnectionSupervisor, :start_link,
         [
           %{
             name: state.gnat_name,
             backoff_period: Context.opt(state.context, :backoff_period, 2_000),
             connection_settings: connection_settings(state.context)
           }
         ]},
      type: :supervisor
    }

    case Supervisor.start_link([child], strategy: :one_for_one) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      error -> error
    end
  end

  defp connection_settings(context) do
    case Context.opt(context, :connection_settings) do
      nil -> [host_settings(context)]
      settings -> List.wrap(settings)
    end
  end

  defp host_settings(context) do
    {host, port} =
      case String.split(context.server.host, ":", parts: 2) do
        [host, port] ->
          case Integer.parse(port) do
            {port, ""} -> {host, port}
            _ -> {host, 4222}
          end

        [host] ->
          {host, 4222}
      end

    %{host: host, port: port}
  end

  defp do_subscribe(state, pid, filter) do
    opts =
      case Context.opt(state.context, :queue_group) do
        nil -> []
        group -> [queue_group: group]
      end

    case safely(fn -> Gnat.sub(pid, self(), filter, opts) end) do
      {:ok, sid} ->
        Logger.debug("AshAsyncApi NATS subscribed to #{filter}")
        {:ok, sid}

      {:error, reason} = error ->
        Logger.debug("AshAsyncApi NATS could not subscribe to #{filter} yet: #{inspect(reason)}")
        error
    end
  end

  # A Gnat process can die between `whereis` and the call, which is a normal race during
  # a reconnect, not something to crash the transport over.
  defp safely(fun) do
    fun.()
  catch
    :exit, reason -> {:error, reason}
  end

  defp deliver(state, message) do
    AshAsyncApi.Transport.deliver(state.context, message.topic, message.body,
      headers: headers(message),
      reply_to: Map.get(message, :reply_to),
      metadata: %{sid: Map.get(message, :sid)}
    )
  rescue
    error ->
      Logger.error("""
      AshAsyncApi failed to handle a NATS message on #{inspect(message.topic)}:
      #{Exception.format(:error, error, __STACKTRACE__)}
      """)
  end

  defp headers(%{headers: headers}) when is_list(headers) do
    Map.new(headers, fn {key, value} -> {to_string(key), value} end)
  end

  defp headers(_message), do: %{}

  defp pub_opts(opts) do
    []
    |> maybe_put(:reply_to, opts[:reply_to])
    |> maybe_put(:headers, nats_headers(opts[:headers]))
  end

  defp nats_headers(nil), do: nil
  defp nats_headers(headers) when map_size(headers) == 0, do: nil

  defp nats_headers(headers) do
    Enum.map(headers, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp gnat_name(%Context{} = context) do
    Module.concat(Context.process_name(context), "Gnat")
  end
end
