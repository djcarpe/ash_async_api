defmodule AshAsyncApi.PublishTest do
  use AshAsyncApi.RouterCase, async: false

  alias AshAsyncApi.Test.Helpdesk
  alias AshAsyncApi.Test.Helpdesk.Comment
  alias AshAsyncApi.Test.Helpdesk.Ticket
  alias AshAsyncApi.Test.Router

  setup do
    start_router!(Router)
    :ok
  end

  describe "automatic publishing via the notifier" do
    test "creating a ticket publishes to the interpolated address" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      ticket = open_ticket(subject: "Printer on fire")

      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.address == "helpdesk/acme/tickets/#{ticket.id}/events"
      assert envelope.channel == :ticket_events
      assert envelope.message == "ticketOpened"
      assert envelope.operation == :ticket_open_send
      assert envelope.resource == Ticket
      assert envelope.action == :open
    end

    test "the payload contains exactly the declared fields" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      ticket = open_ticket(subject: "Printer on fire", priority: :urgent)

      assert_receive {:ash_async_api, envelope}, 500

      assert Map.keys(envelope.payload) |> Enum.sort() ==
               [:id, :opened_at, :priority, :status, :subject]

      assert envelope.payload.id == ticket.id
      assert envelope.payload.subject == "Printer on fire"
      assert envelope.payload.priority == "urgent"
    end

    test "values are dumped to JSON-safe terms" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      open_ticket(subject: "Cost query") |> close_ticket()

      # ticketOpened first, then ticketClose.
      assert_receive {:ash_async_api, _opened}, 500
      assert_receive {:ash_async_api, closed}, 500

      assert is_binary(closed.payload.opened_at)
      assert {:ok, _, _} = DateTime.from_iso8601(closed.payload.opened_at)
      assert closed.payload.status == "closed"
    end

    test "hidden fields never reach the payload" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      open_ticket(subject: "Secret") |> close_ticket()

      assert_receive {:ash_async_api, _opened}, 500
      assert_receive {:ash_async_api, closed}, 500

      refute Map.has_key?(closed.payload, :internal_notes)
    end

    test "nil fields are dropped by default" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      open_ticket(subject: "No body") |> close_ticket()

      assert_receive {:ash_async_api, _opened}, 500
      assert_receive {:ash_async_api, closed}, 500

      refute Map.has_key?(closed.payload, :body)
      refute Map.has_key?(closed.payload, :estimated_cost)
    end

    test "an address subscriber only hears about its own ticket" do
      one = open_ticket(subject: "One")
      two = open_ticket(subject: "Two")

      AshAsyncApi.subscribe(Router, "helpdesk/acme/tickets/#{one.id}/events")

      close_ticket(two)
      refute_receive {:ash_async_api, _}, 200

      close_ticket(one)
      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.payload.id == one.id
    end

    test "an action with no publish operation publishes nothing" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      ticket = open_ticket(subject: "To be destroyed")
      assert_receive {:ash_async_api, _opened}, 500

      Ash.destroy!(ticket)
      refute_receive {:ash_async_api, _}, 200
    end

    test "domain-declared operations publish too" do
      AshAsyncApi.subscribe(Router, :comment_events)

      ticket = open_ticket(subject: "Needs comment")

      comment =
        Comment
        |> Ash.Changeset.for_create(:add, %{
          ticket_id: ticket.id,
          author: "dj",
          body: "Looking into it"
        })
        |> Ash.create!()

      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.channel == :comment_events
      assert envelope.message == "commentAdded"

      assert envelope.address ==
               "helpdesk/acme/tickets/#{ticket.id}/comments/#{comment.id}"

      assert envelope.payload.id == comment.id
    end
  end

  describe "relationship paths in addresses" do
    test "a belongs_to's own key is read without loading the relationship" do
      AshAsyncApi.subscribe(Router, :comment_events)

      ticket = open_ticket(subject: "Has comments")
      comment = add_comment(ticket, "dj", "first")

      assert_receive {:ash_async_api, envelope}, 500

      # `[:ticket, :id]` is the foreign key already on the comment.
      assert envelope.params.ticket_id == ticket.id
      assert envelope.address =~ "/tickets/#{ticket.id}/comments/#{comment.id}"
    end

    test "a field beyond the foreign key is loaded through the relationship" do
      AshAsyncApi.subscribe(Router, :comment_events)

      ticket = open_ticket(subject: "Has comments")
      add_comment(ticket, "dj", "first")

      assert_receive {:ash_async_api, envelope}, 500

      # `[:ticket, :organization_id]` is on the ticket, not the comment, so the publisher
      # had to load it.
      assert envelope.params.ticket_organization_id == ticket.organization_id

      assert envelope.address ==
               "helpdesk/acme/tickets/#{ticket.id}/comments/#{envelope.payload.id}"
    end

    test "a parameter block that only documents does not change where the value is read" do
      # `:ticket_organization_id` is documented on the channel with no `source`. If that
      # documentation were treated as a source, the publisher would look for a field of that
      # name on the comment — there is none, and the address would be unfillable.
      channel =
        Router.__ash_async_api__()
        |> AshAsyncApi.Router.Table.channel(:comment_events)

      assert AshAsyncApi.Router.Table.ResolvedChannel.parameter(channel, :ticket_organization_id)
      assert channel.compiled.param_paths[:ticket_organization_id] == [:ticket, :organization_id]

      ticket = open_ticket(subject: "Documented params")
      comment = add_comment(ticket, "dj", "body")

      assert %{ticket_organization_id: "acme"} =
               AshAsyncApi.Publisher.address_params(channel, comment, domain: Helpdesk)
    end

    test "a relationship that cannot be resolved reports the path it came from" do
      channel =
        Router.__ash_async_api__()
        |> AshAsyncApi.Router.Table.channel(:comment_events)

      # A comment struct with no ticket at all.
      assert %{ticket_organization_id: nil} =
               AshAsyncApi.Publisher.address_params(channel, %Comment{id: "x"}, domain: Helpdesk)
    end
  end

  describe "filters" do
    test "a filter that returns false suppresses the message" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      ticket = open_ticket(subject: "Escalate me", priority: :urgent)
      assert_receive {:ash_async_api, _opened}, 500

      # `escalate` sets priority to :urgent, so the filter passes.
      ticket
      |> Ash.Changeset.for_update(:escalate, %{})
      |> Ash.update!()

      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.message == "ticketEscalated"
    end
  end

  describe "publish/3 explicitly" do
    test "publishes a record for a named action" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      ticket = open_ticket(subject: "Manual")
      assert_receive {:ash_async_api, _from_notifier}, 500

      assert {:ok, [envelope]} = AshAsyncApi.publish(Router, ticket, action: :close)
      assert envelope.message == "ticketClose"

      assert_receive {:ash_async_api, received}, 500
      assert received.id == envelope.id
    end

    test "returns a helpful error for an action with no publish operation" do
      ticket = open_ticket(subject: "No op")

      assert {:error, %AshAsyncApi.Error.UnknownOperation{} = error} =
               AshAsyncApi.publish(Router, ticket, action: :read)

      message = Exception.message(error)
      assert message =~ "No publish operation"
      assert message =~ "Ticket.read"
    end

    test "returns a helpful error when an address parameter has no value" do
      # A ticket struct with no id cannot fill in {ticket_id}.
      assert {:error, %AshAsyncApi.Error.MissingAddressParams{} = error} =
               AshAsyncApi.publish(Router, %Ticket{subject: "No id"}, action: :close)

      message = Exception.message(error)
      assert message =~ "missing values for: [:id]"
      # The error names the path each missing value was read from.
      assert message =~ "id <- [:id]"
    end
  end

  describe "publish_to/4" do
    test "publishes an arbitrary payload onto a channel" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      assert {:ok, envelope} =
               AshAsyncApi.publish_to(Router, :ticket_events, %{note: "hello"},
                 params: %{organization_id: "acme", id: 42},
                 message: "adHoc"
               )

      assert envelope.address == "helpdesk/acme/tickets/42/events"

      assert_receive {:ash_async_api, received}, 500
      assert received.payload == %{note: "hello"}
      assert received.message == "adHoc"
    end

    test "errors for an unknown channel" do
      assert {:error, %AshAsyncApi.Error.UnknownChannel{} = error} =
               AshAsyncApi.publish_to(Router, :nope, %{})

      assert Exception.message(error) =~ "Unknown channel :nope"
    end

    test "errors when address parameters are missing" do
      assert {:error, %AshAsyncApi.Error.MissingAddressParams{}} =
               AshAsyncApi.publish_to(Router, :ticket_events, %{})
    end

    test "an untemplated channel needs no params" do
      AshAsyncApi.subscribe(Router, :ticket_commands)

      assert {:ok, envelope} = AshAsyncApi.publish_to(Router, :ticket_commands, %{do: "thing"})
      assert envelope.address == "helpdesk/tickets/commands"

      assert_receive {:ash_async_api, _}, 500
    end
  end

  describe "origin header" do
    test "every published message is stamped with the router and node" do
      assert {:ok, envelope} =
               AshAsyncApi.publish_to(Router, :ticket_commands, %{})

      assert AshAsyncApi.Envelope.get_header(envelope, "ash-async-api-origin") ==
               "AshAsyncApi.Test.Router@#{node()}"

      assert AshAsyncApi.Publisher.own_message?(Router, envelope)
    end

    test "a message from a different router is not our own" do
      assert {:ok, envelope} = AshAsyncApi.publish_to(Router, :ticket_commands, %{})

      refute AshAsyncApi.Publisher.own_message?(AshAsyncApi.Test.LoopbackRouter, envelope)
    end

    test "a message from a node outside the cluster is not our own" do
      envelope =
        AshAsyncApi.Envelope.new(channel: :ticket_commands)
        |> AshAsyncApi.Envelope.put_header(
          "ash-async-api-origin",
          "AshAsyncApi.Test.Router@other@example.com"
        )

      refute AshAsyncApi.Publisher.own_message?(Router, envelope)
    end

    test "a message with no origin header is not our own" do
      refute AshAsyncApi.Publisher.own_message?(Router, AshAsyncApi.Envelope.new(%{}))
    end
  end

  describe "transports?: false" do
    test "keeps a message inside the cluster" do
      AshAsyncApi.subscribe(Router, :ticket_commands)

      assert {:ok, _envelope} =
               AshAsyncApi.publish_to(Router, :ticket_commands, %{}, transports?: false)

      assert_receive {:ash_async_api, _}, 500
    end
  end

  defp open_ticket(attrs) do
    Ticket
    |> Ash.Changeset.for_create(:open, Map.new(attrs))
    |> Ash.create!()
  end

  defp add_comment(ticket, author, body) do
    Comment
    |> Ash.Changeset.for_create(:add, %{ticket_id: ticket.id, author: author, body: body})
    |> Ash.create!()
  end

  defp close_ticket(ticket) do
    ticket
    |> Ash.Changeset.for_update(:close, %{})
    |> Ash.update!()
  end
end
