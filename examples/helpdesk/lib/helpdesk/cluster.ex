defmodule Helpdesk.Cluster do
  @moduledoc """
  Connects to the other nodes named in `CLUSTER_NODES`, retrying until they answer.

  `Group` discovers its peers through `:net_kernel.monitor_nodes/1`, so all it needs is an
  ordinary Erlang distribution connection. There is no cluster configuration in
  AshAsyncApi itself — `libcluster` would do just as well here.
  """

  use GenServer

  require Logger

  @interval 2_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    {:ok, %{}, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    connect()
    {:noreply, state}
  end

  @impl true
  def handle_info(:connect, state) do
    connect()
    {:noreply, state}
  end

  defp connect do
    missing = Enum.reject(peers(), &(&1 in Node.list()))

    for peer <- missing do
      if Node.connect(peer) == true do
        Logger.info("[cluster] connected to #{peer}")
      end
    end

    # Keep retrying: peers start at their own pace, and a dropped connection should heal.
    Process.send_after(self(), :connect, @interval)
  end

  defp peers do
    System.get_env("CLUSTER_NODES", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom(String.trim(&1)))
    |> Enum.reject(&(&1 == node()))
  end
end
