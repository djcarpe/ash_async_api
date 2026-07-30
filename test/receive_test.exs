defmodule AshAsyncApi.ReceiveTest do
  use AshAsyncApi.RouterCase, async: false

  alias AshAsyncApi.Test.Helpdesk
  alias AshAsyncApi.Test.Helpdesk.Comment
  alias AshAsyncApi.Test.Helpdesk.Ticket
  alias AshAsyncApi.Test.LoopbackRouter, as: Router
  alias AshAsyncApi.Transport.Context

  setup do
    # `AshAsyncApi.Transport.Local` needs no connection process, so transports can be
    # started: nothing is dialled.
    start_router!(Router, start_transports?: true)

    # The notifier publishes through the configured `AshAsyncApi.Test.Router`, so it has
    # to be running too or every create logs a "not running" error.
    start_router!(AshAsyncApi.Test.Router, start_transports?: true)

    :ok
  end

  describe "inbound messages run subscribe operations" do
    test "a message on the command channel creates a ticket" do
      deliver("helpdesk/tickets/commands", %{
        "message" => "openTicket",
        "payload" => %{"subject" => "From MQTT", "priority" => "urgent"}
      })

      assert eventually(fn -> Ash.read!(Ticket, domain: Helpdesk) != [] end)

      assert [ticket] = Ash.read!(Ticket, domain: Helpdesk)
      assert ticket.subject == "From MQTT"
      assert ticket.priority == :urgent
      assert ticket.status == :open
    end

    test "a bare payload with no envelope wrapper works" do
      # Messages from systems that know nothing about AshAsyncApi arrive like this.
      deliver("helpdesk/tickets/commands", %{"subject" => "Foreign producer"})

      assert eventually(fn -> Ash.read!(Ticket, domain: Helpdesk) != [] end)
      assert [%{subject: "Foreign producer"}] = Ash.read!(Ticket, domain: Helpdesk)
    end

    test "camelCase payload keys are matched to snake_case action inputs" do
      deliver("helpdesk/tickets/commands", %{
        "payload" => %{"subject" => "Camel", "internalNotes" => "ignored"}
      })

      assert eventually(fn -> Ash.read!(Ticket, domain: Helpdesk) != [] end)
      assert [ticket] = Ash.read!(Ticket, domain: Helpdesk)
      assert ticket.subject == "Camel"
    end

    test "fields the action does not accept are dropped, not passed through" do
      deliver("helpdesk/tickets/commands", %{
        "payload" => %{"subject" => "Safe", "status" => "closed", "id" => Ash.UUID.generate()}
      })

      assert eventually(fn -> Ash.read!(Ticket, domain: Helpdesk) != [] end)
      assert [ticket] = Ash.read!(Ticket, domain: Helpdesk)

      # `:open` accepts only [:subject, :body, :priority], so `status` could not be set.
      assert ticket.status == :open
    end

    test "a message naming a different message type does not run the operation" do
      deliver("helpdesk/tickets/commands", %{
        "message" => "somethingElse",
        "payload" => %{"subject" => "Should not exist"}
      })

      Process.sleep(200)
      assert Ash.read!(Ticket, domain: Helpdesk) == []
    end

    test "an address matching no channel is ignored" do
      assert {:ok, []} = deliver("some/other/topic", %{"payload" => %{}})
    end

    test "a payload the action rejects does not crash the transport" do
      # `subject` is required, so the create fails; delivery must still return cleanly.
      assert {:ok, [_envelope]} = deliver("helpdesk/tickets/commands", %{"payload" => %{}})

      Process.sleep(200)
      assert Ash.read!(Ticket, domain: Helpdesk) == []
    end
  end

  describe "address parameters become action input" do
    test "a parameter from the address is used when the payload omits it" do
      ticket = open_ticket("Needs a comment")

      assert {:ok, [envelope]} =
               deliver("helpdesk/tickets/#{ticket.id}/comments/new", %{
                 "message" => "addComment",
                 "payload" => %{"author" => "dj", "body" => "From another service"}
               })

      assert envelope.params == %{ticket_id: ticket.id}

      assert eventually(fn -> Ash.read!(Comment, domain: Helpdesk) != [] end)
      assert [comment] = Ash.read!(Comment, domain: Helpdesk)

      # The payload never mentioned ticket_id; it came out of the address.
      assert comment.ticket_id == ticket.id
      assert comment.author == "dj"
      assert comment.body == "From another service"
    end
  end

  describe "reply_channel" do
    test "the action result is published back on the reply channel" do
      AshAsyncApi.subscribe(Router, :ticket_events)

      deliver("helpdesk/tickets/commands", %{
        "message" => "openTicket",
        "payload" => %{"subject" => "Wants a reply"}
      })

      # Two messages land on ticket_events: the notifier's ticketOpened, and the reply.
      envelopes = collect_messages(2)

      assert Enum.any?(envelopes, &(&1.message == "openTicketReply"))

      reply = Enum.find(envelopes, &(&1.message == "openTicketReply"))
      assert reply.payload.subject == "Wants a reply"
      assert reply.correlation_id
    end
  end

  describe "ignore_own_messages?" do
    test "the loopback router processes its own messages, because it opted in" do
      assert {:ok, [_envelope]} =
               deliver("helpdesk/tickets/commands", %{"payload" => %{"subject" => "Loopback"}})
    end

    test "the default router drops its own messages" do
      envelope =
        AshAsyncApi.Envelope.new(payload: %{"subject" => "Should be dropped"})
        |> AshAsyncApi.Envelope.put_header(
          "ash-async-api-origin",
          AshAsyncApi.Publisher.origin(AshAsyncApi.Test.Router)
        )

      context = context(AshAsyncApi.Test.Router)
      body = Jason.encode!(AshAsyncApi.Envelope.to_wire(envelope))

      assert {:ok, []} =
               AshAsyncApi.Transport.deliver(context, "helpdesk/tickets/commands", body)
    end
  end

  describe "fan-out to subscribers" do
    test "an inbound message reaches subscribers as well as running the action" do
      AshAsyncApi.subscribe(Router, "helpdesk/tickets/commands")

      deliver("helpdesk/tickets/commands", %{"payload" => %{"subject" => "Both"}})

      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.address == "helpdesk/tickets/commands"
      assert envelope.channel == :ticket_commands
      assert envelope.server == :cluster

      assert eventually(fn -> Ash.read!(Ticket, domain: Helpdesk) != [] end)
    end

    test "broker headers and envelope headers are merged, envelope winning" do
      AshAsyncApi.subscribe(Router, "helpdesk/tickets/commands")

      wire = %{"payload" => %{"subject" => "Headers"}, "headers" => %{"from" => "envelope"}}

      AshAsyncApi.Transport.deliver(
        context(Router),
        "helpdesk/tickets/commands",
        Jason.encode!(wire),
        headers: %{"from" => "broker", "only-broker" => "yes"}
      )

      assert_receive {:ash_async_api, envelope}, 500
      assert AshAsyncApi.Envelope.get_header(envelope, "from") == "envelope"
      assert AshAsyncApi.Envelope.get_header(envelope, "only-broker") == "yes"
    end

    test "transport metadata is carried on the envelope" do
      AshAsyncApi.subscribe(Router, "helpdesk/tickets/commands")

      AshAsyncApi.Transport.deliver(
        context(Router),
        "helpdesk/tickets/commands",
        Jason.encode!(%{"payload" => %{"subject" => "Meta"}}),
        metadata: %{offset: 42, partition: 3}
      )

      assert_receive {:ash_async_api, envelope}, 500
      assert envelope.metadata == %{offset: 42, partition: 3}
    end
  end

  describe "the envelope is available to the action" do
    test "actions reached via a subscribe operation can see the envelope" do
      # `AshAsyncApi.envelope/1` reads it out of the Ash action context.
      context = %{ash_async_api: %{envelope: AshAsyncApi.Envelope.new(address: "a/b")}}

      assert AshAsyncApi.envelope(context).address == "a/b"
      assert AshAsyncApi.envelope(%{context: context}).address == "a/b"
      assert AshAsyncApi.envelope(%{}) == nil
    end
  end

  defp deliver(address, wire) do
    AshAsyncApi.Transport.deliver(context(Router), address, Jason.encode!(wire))
  end

  defp context(router) do
    server = AshAsyncApi.Domain.Info.server(Helpdesk, :cluster)

    Context.new(router, Helpdesk, server)
  end

  defp open_ticket(subject) do
    Ticket
    |> Ash.Changeset.for_create(:open, %{subject: subject})
    |> Ash.create!()
  end

  defp collect_messages(count, acc \\ [])
  defp collect_messages(0, acc), do: Enum.reverse(acc)

  defp collect_messages(count, acc) do
    receive do
      {:ash_async_api, envelope} -> collect_messages(count - 1, [envelope | acc])
    after
      1_000 -> Enum.reverse(acc)
    end
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end
end
