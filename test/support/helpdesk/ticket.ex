defmodule AshAsyncApi.Test.Helpdesk.Ticket do
  @moduledoc "A ticket, publishing lifecycle events and accepting commands."

  use Ash.Resource,
    domain: AshAsyncApi.Test.Helpdesk,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource]

  ets do
    private? true
  end

  async_api do
    type "ticket"

    hide_fields [:internal_notes]

    channels do
      channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
        description "Lifecycle events for a single ticket"

        parameter :ticket_id do
          source :id
          description "The id of the ticket"
        end
      end

      channel :ticket_commands, "helpdesk/tickets/commands" do
        description "Commands other services send to the helpdesk"
      end
    end

    operations do
      publish :open, :ticket_events do
        message_name "ticketOpened"
        summary "A ticket was opened"
        payload_fields [:id, :subject, :status, :priority, :opened_at]
      end

      # No `message_name`, so it defaults to a camelized `<type>_<action>`: `ticketClose`.
      publish :close, :ticket_events do
        summary "A ticket was closed"
      end

      publish :escalate, :ticket_events do
        message_name "ticketEscalated"
        # Only publish the escalation that actually raised the priority.
        filter fn ticket, _context -> ticket.priority == :urgent end
      end

      subscribe :open, :ticket_commands do
        message_name "openTicket"
        reply_channel :ticket_events
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

    attribute :estimated_cost, :decimal, public?: true
  end

  actions do
    defaults [:read, :destroy]

    create :open do
      primary? true
      description "Open a new ticket"
      accept [:subject, :body, :priority]
    end

    update :close do
      description "Close a ticket"
      accept []
      change set_attribute(:status, :closed)
    end

    update :escalate do
      description "Raise a ticket's priority"
      accept []
      change set_attribute(:priority, :urgent)
    end
  end
end
