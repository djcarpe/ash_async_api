defmodule Helpdesk.Support.Ticket do
  @moduledoc """
  A ticket.

  Storage is ETS, which is deliberately **per node**. That is what makes this demo
  convincing: when node2 logs a ticket that only exists in node1's ETS table, node2 can
  only have learned about it from the message. A shared database would leave you unable to
  tell whether the subscriber received anything or just read the row.
  """

  use Ash.Resource,
    domain: Helpdesk.Support,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource]

  ets do
    private? false
  end

  async_api do
    type "ticket"

    # Never put this on the wire, and never let a message set it. `hide_fields` applies to
    # outbound payloads and to inbound action input alike.
    hide_fields [:internal_notes]

    channels do
      # One address per ticket, so a consumer can watch a single ticket rather than
      # every ticket. Carried by MQTT.
      channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
        description "Lifecycle events for a single ticket"
        servers [:mqtt]

        parameter :ticket_id do
          source :id
          description "The id of the ticket"
        end
      end

      # Commands come in over NATS, where a queue group gives us exactly-once handling
      # across the cluster. Note the `.` separators — AshAsyncApi detects them and
      # subscribes with NATS' `*` wildcard.
      channel :ticket_commands, "helpdesk.tickets.commands" do
        description "Commands other services send to the helpdesk"
        servers [:nats]
      end
    end

    operations do
      publish :open, :ticket_events do
        message_name "ticketOpened"
        summary "A ticket was opened"
        payload_fields [:id, :subject, :status, :priority, :opened_at]

        # MQTT-specific delivery settings, per the AsyncAPI MQTT binding. These are
        # honoured at runtime *and* rendered into the generated document.
        bindings %{mqtt: %{qos: 1}}
      end

      publish :close, :ticket_events do
        message_name "ticketClosed"
        summary "A ticket was closed"
      end

      publish :escalate, :ticket_events do
        message_name "ticketEscalated"
        summary "A ticket was escalated to urgent"

        # Escalating an already-urgent ticket is a no-op, and a no-op is not an event.
        #
        # The filter receives the updated record and the Ash notification, and
        # `notification.changeset.data` is the record as it was *before* the update — so
        # comparing the two is what tells you whether anything actually moved.
        # (`changing_attribute?/2` is not the right question here: `set_attribute` marks the
        # attribute as changed whether or not the value differs.)
        filter fn ticket, notification ->
          ticket.priority == :urgent and notification.changeset.data.priority != :urgent
        end
      end

      # An inbound message runs this action. The payload is narrowed to what `:open`
      # accepts, so a message cannot set `status` or `internal_notes`.
      subscribe :open, :ticket_commands do
        message_name "openTicket"
        summary "Ask the helpdesk to open a ticket"
      end
    end
  end

  attributes do
    uuid_primary_key :id

    attribute :subject, :string do
      allow_nil? false
      public? true
      constraints max_length: 200
    end

    attribute :body, :string, public?: true

    attribute :status, :atom do
      allow_nil? false
      public? true
      default :open
      constraints one_of: [:open, :closed]
    end

    attribute :priority, :atom do
      allow_nil? false
      public? true
      default :normal
      constraints one_of: [:low, :normal, :urgent]
    end

    attribute :internal_notes, :string, public?: true

    attribute :opened_at, :utc_datetime_usec do
      public? true
      default &DateTime.utc_now/0
    end
  end

  actions do
    defaults [:read, :destroy]

    create :open do
      primary? true
      description "Open a new ticket"

      # `internal_notes` is accepted here, but it is in `hide_fields`, which means two
      # things: it is stripped from published payloads, *and* it is stripped from inbound
      # message payloads. Local code can set it; a message from outside cannot.
      accept [:subject, :body, :priority, :internal_notes]
    end

    update :close do
      description "Close a ticket"
      accept []
      change set_attribute(:status, :closed)
    end

    update :escalate do
      description "Raise a ticket's priority to urgent"
      accept []
      change set_attribute(:priority, :urgent)
    end
  end
end
