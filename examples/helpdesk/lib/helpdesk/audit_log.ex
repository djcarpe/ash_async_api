defmodule Helpdesk.AuditLog do
  @moduledoc """
  A plain GenServer that subscribes to every ticket event and logs it.

  This process is the point of the whole demo. It runs on **both** nodes, holds no data,
  and has no broker connection of its own. When you open a ticket on node1 and node2's
  audit log prints it, the only path that message could have taken is
  `AshAsyncApi.PubSub` — that is, `Group`, over Erlang distribution.

  Note the order in `init/1`: subscribe first, then read state. `Group` is eventually
  consistent, so reading first would leave a window in which an event could be missed.
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "The events this node has seen, most recent first."
  def entries, do: GenServer.call(__MODULE__, :entries)

  @impl true
  def init(_opts) do
    :ok = AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, :ticket_events, %{role: :audit})
    :ok = AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, :ticket_commands, %{role: :audit})

    Logger.info("[audit] subscribed on #{node()}")

    {:ok, []}
  end

  @impl true
  def handle_call(:entries, _from, entries), do: {:reply, entries, entries}

  @impl true
  def handle_info({:ash_async_api, envelope}, entries) do
    Logger.info("""
    [audit] #{envelope.message || "message"} on #{node()}
      address: #{envelope.address}
      payload: #{inspect(envelope.payload)}
      origin:  #{AshAsyncApi.Envelope.get_header(envelope, "ash-async-api-origin")}
    """)

    {:noreply, Enum.take([envelope | entries], 100)}
  end
end
