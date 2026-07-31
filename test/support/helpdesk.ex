defmodule AshAsyncApi.Test.Helpdesk do
  @moduledoc "A domain with an AsyncAPI description, one broker-less server, and domain-level operations."

  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    id "urn:com:example:helpdesk"

    info do
      title "Helpdesk Events"
      version "2.1.0"
      description "Everything that happens to a ticket."
      contact_name "Helpdesk Team"
      contact_email "helpdesk@example.com"
      license_name "MIT"
      tags ["helpdesk"]
    end

    servers do
      server :cluster, "erlang-distribution" do
        protocol :erlang
        description "In-cluster delivery, no broker involved"
        transport AshAsyncApi.Transport.Local
      end
    end

    channels do
      # `[:ticket, :id]` is a belongs_to's own key, so it needs no query.
      # `[:ticket, :organization_id]` is not, so the publisher loads the ticket.
      channel :comment_events,
              [
                "helpdesk",
                [:ticket, :organization_id],
                "tickets",
                [:ticket, :id],
                "comments",
                :id
              ] do
        description "Comments added to a ticket"

        parameter :ticket_organization_id do
          description "The organization the comment's ticket belongs to"
        end

        parameter :ticket_id do
          description "The id of the ticket the comment belongs to"
        end
      end

      channel :comment_commands, [
        "helpdesk",
        "tickets",
        {:ticket_id, [:ticket, :id]},
        "comments",
        "new"
      ] do
        description "Requests from other services to comment on a ticket"

        parameter :ticket_id do
          description "The id of the ticket to comment on"
        end
      end
    end

    operations do
      publish AshAsyncApi.Test.Helpdesk.Comment, :add, :comment_events do
        message_name "commentAdded"
        summary "A comment was added to a ticket"
      end

      # `ticket_id` is not in the payload — it comes out of the channel address.
      subscribe AshAsyncApi.Test.Helpdesk.Comment, :add, :comment_commands do
        name :comment_add_command
        message_name "addComment"
      end
    end
  end

  resources do
    resource AshAsyncApi.Test.Helpdesk.Ticket
    resource AshAsyncApi.Test.Helpdesk.Comment
  end
end
