defmodule AshAsyncApi.Transport.Mqtt do
  @moduledoc """
  MQTT transport, built on [`emqtt`](https://hex.pm/packages/emqtt).

  MQTT's topic model lines up with AsyncAPI channel addresses almost exactly: `/`
  separated levels, and a `+` single-level wildcard that is precisely what a
  `{parameter}` means. So `helpdesk/tickets/{ticket_id}/events` subscribes as
  `helpdesk/tickets/+/events` and the ticket id comes back out of the concrete topic.

  ## Setup

  Add the client library — it is not a dependency of AshAsyncApi, so you only carry it
  if you use MQTT:

      {:emqtt, "~> 1.13"}

  Then declare the server:

      servers do
        server :mqtt, "broker.example.com:1883" do
          protocol :mqtt
          protocol_version "5"
          transport AshAsyncApi.Transport.Mqtt
          transport_opts [
            clientid: "helpdesk",
            username: "helpdesk",
            password: {:system, "MQTT_PASSWORD"},
            clean_start: false
          ]
        end
      end

  ## Options

  Everything in `transport_opts` is passed through to `:emqtt.start_link/1` after
  `:host` and `:port` are filled in from the server's `host`. A few are handled here:

    * `:qos` — the default QoS for published messages. Defaults to `1` (at least
      once), which is the right default for events you care about.
    * `:retain` — whether published messages are retained. Defaults to `false`.
    * `:subscribe_qos` — the QoS to subscribe with. Defaults to `:qos`.
    * `:reconnect_interval` — milliseconds between reconnection attempts. Defaults to
      `5_000`.

  A `{:system, "VAR"}` tuple in any option is read from the environment at startup,
  so credentials stay out of compiled code.

  ## Per-message overrides

  Channel and operation `bindings` under the `:mqtt` key override the defaults per
  message, following the [MQTT
  binding](https://github.com/asyncapi/bindings/tree/master/mqtt) spec:

      channel :ticket_snapshots, "helpdesk/tickets/{ticket_id}" do
        bindings %{mqtt: %{qos: 1, retain: true}}
      end
  """

  use AshAsyncApi.Transport

  alias AshAsyncApi.Transport.Context

  @impl true
  def wildcard_style, do: {:single, "+"}

  @impl true
  def validate_opts(_server, opts) do
    cond do
      not Code.ensure_loaded?(:emqtt) ->
        {:error,
         """
         The :emqtt library is required to use #{inspect(__MODULE__)}. Add it to your mix.exs:

             {:emqtt, "~> 1.13"}
         """}

      not is_nil(opts[:port]) and not is_integer(opts[:port]) ->
        {:error, "expected :port to be an integer, got: #{inspect(opts[:port])}"}

      true ->
        :ok
    end
  end

  @impl true
  def child_spec(%Context{} = context) do
    %{
      id: Context.process_name(context),
      start: {AshAsyncApi.Transport.Mqtt.Connection, :start_link, [context]},
      type: :worker
    }
  end

  @impl true
  def publish(%Context{} = context, address, body, opts) do
    AshAsyncApi.Transport.Mqtt.Connection.publish(context, address, body, opts)
  end

  @impl true
  def subscribe(%Context{} = context, filter) do
    AshAsyncApi.Transport.Mqtt.Connection.subscribe(context, filter)
  end

  @impl true
  def unsubscribe(%Context{} = context, filter) do
    AshAsyncApi.Transport.Mqtt.Connection.unsubscribe(context, filter)
  end
end
