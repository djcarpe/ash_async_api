defmodule AshAsyncApi.Transport.Local do
  @moduledoc """
  Delivery over the Erlang cluster, with no broker.

  `AshAsyncApi.PubSub` already fans every published message out to every subscriber on
  every node, so for pure event distribution inside one cluster there is nothing left
  for a transport to do — which is why this one starts no processes and its
  `publish/4` does not touch a socket.

  What it *does* add is the receive side. `publish/4` loops the message back through
  `AshAsyncApi.Transport.deliver/4` so that `subscribe` operations run, letting you
  exercise a full publish → action round trip with no infrastructure. That makes it
  the transport to use in tests, and a legitimate choice in production for a system
  whose messages never need to leave the cluster.

      servers do
        server :cluster, "erlang-distribution" do
          protocol :erlang
          transport AshAsyncApi.Transport.Local
        end
      end

  ## Loopback and `ignore_own_messages?`

  The loopback obeys the router's `ignore_own_messages?` setting, which defaults to
  `true` — so by default a message this router published will *not* trigger this
  router's `subscribe` operations. That default is deliberate: a channel that a single
  router both publishes to and subscribes from is an infinite loop waiting to happen.

  Within one application you do not need a message to call your own action — call it.
  The `subscribe` side is for messages from somewhere else. To exercise it in a test,
  start the router with `ignore_own_messages?: false`.
  """

  use AshAsyncApi.Transport

  @impl true
  def child_spec(_context), do: nil

  @impl true
  def wildcard_style, do: {:single, "+"}

  @impl true
  def publish(context, address, body, opts) do
    case AshAsyncApi.Transport.deliver(context, address, IO.iodata_to_binary(body),
           # `AshAsyncApi.Publisher` has already broadcast this envelope; broadcasting
           # again here would deliver it to every subscriber twice.
           broadcast?: false,
           headers: opts[:headers],
           content_type: opts[:content_type],
           correlation_id: opts[:correlation_id],
           reply_to: opts[:reply_to]
         ) do
      {:ok, _envelopes} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def subscribe(_context, _filter), do: :ok

  @impl true
  def unsubscribe(_context, _filter), do: :ok
end
