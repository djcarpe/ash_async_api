defmodule Helpdesk.Support do
  @moduledoc """
  The domain. Holds the AsyncAPI document metadata and the two brokers.

  Splitting the traffic across two brokers is not showing off for its own sake — it
  demonstrates that nothing above `AshAsyncApi.Transport` cares which broker a channel
  uses. The same `AshAsyncApi.subscribe/2` call sees events that arrived over MQTT and
  commands that arrived over NATS.
  """

  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    id "urn:com:example:helpdesk"

    info do
      title "Helpdesk Events"
      version "1.0.0"

      description """
      Everything that happens to a helpdesk ticket.

      Ticket events are published to MQTT, one topic per ticket. Commands are consumed
      from NATS under a queue group, so exactly one node in the cluster handles each one.
      """

      contact_name "Helpdesk Team"
      contact_email "helpdesk@example.com"
      license_name "MIT"
      tags ["helpdesk"]
    end

    servers do
      server :mqtt, "mosquitto:1883" do
        protocol :mqtt
        protocol_version "5"
        description "Outbound ticket events"
        transport AshAsyncApi.Transport.Mqtt

        transport_opts clientid: {:system, "MQTT_CLIENT_ID", "helpdesk"},
                       qos: 1,
                       clean_start: true
      end

      server :nats, "nats:4222" do
        protocol :nats
        description "Inbound ticket commands"
        transport AshAsyncApi.Transport.Nats

        # The queue group is the important part. Without it, both nodes would receive
        # every command and both would open a ticket.
        transport_opts queue_group: "helpdesk"
      end
    end
  end

  resources do
    resource Helpdesk.Support.Ticket
  end
end
