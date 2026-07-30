defmodule AshAsyncApi.Transport.Nats do
  @moduledoc """
  NATS transport, built on [`gnat`](https://hex.pm/packages/gnat).

  NATS subjects are `.` separated with a `*` single-token wildcard, so a channel
  address written NATS-style works directly:

      channel :ticket_events, "helpdesk.tickets.{ticket_id}.events"

  subscribes as `helpdesk.tickets.*.events`. `AshAsyncApi.Address` detects the
  separator from the template, so `/` separated addresses also work — NATS accepts
  them as literal tokens, they just cannot be wildcarded per level.

  ## Setup

      {:gnat, "~> 1.8"}

      servers do
        server :nats, "nats.example.com:4222" do
          protocol :nats
          transport AshAsyncApi.Transport.Nats
          transport_opts [
            connection_settings: [%{host: "nats.example.com", port: 4222}],
            queue_group: "helpdesk"
          ]
        end
      end

  ## Options

    * `:connection_settings` — passed to `Gnat.ConnectionSupervisor`. Derived from the
      server's `host` when omitted.
    * `:queue_group` — the NATS queue group to subscribe under. Strongly recommended:
      with a queue group, one member of the group handles each message, so running
      three nodes does not run every `subscribe` action three times. Without one, every
      node receives every message.
    * `:reply_timeout` — milliseconds to wait for a request/reply response. Defaults to
      `5_000`.

  ## Queue groups and delivery

  When `:queue_group` is set, NATS itself does the deduplication, so inbound messages
  arrive on exactly one node and `AshAsyncApi.PubSub` fans them out from there — the
  default `:cluster` scope is correct. Without a queue group every node gets its own
  copy, and each node's `subscribe` operations run: usually not what you want.
  """

  use AshAsyncApi.Transport

  alias AshAsyncApi.Transport.Context

  @impl true
  def wildcard_style, do: {:single, "*"}

  @impl true
  def validate_opts(_server, _opts) do
    if Code.ensure_loaded?(Gnat) do
      :ok
    else
      {:error,
       """
       The :gnat library is required to use #{inspect(__MODULE__)}. Add it to your mix.exs:

           {:gnat, "~> 1.8"}
       """}
    end
  end

  @impl true
  def child_spec(%Context{} = context) do
    %{
      id: Context.process_name(context),
      start: {AshAsyncApi.Transport.Nats.Connection, :start_link, [context]},
      type: :supervisor
    }
  end

  @impl true
  def publish(%Context{} = context, address, body, opts) do
    AshAsyncApi.Transport.Nats.Connection.publish(context, address, body, opts)
  end

  @impl true
  def subscribe(%Context{} = context, filter) do
    AshAsyncApi.Transport.Nats.Connection.subscribe(context, filter)
  end

  @impl true
  def unsubscribe(%Context{} = context, filter) do
    AshAsyncApi.Transport.Nats.Connection.unsubscribe(context, filter)
  end
end
