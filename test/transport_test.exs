defmodule AshAsyncApi.TransportTest do
  use ExUnit.Case, async: true

  doctest AshAsyncApi.Transport.Kafka

  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Test.Helpdesk
  alias AshAsyncApi.Test.Router
  alias AshAsyncApi.Transport

  describe "wildcard_style/1" do
    test "each transport declares its own broker syntax" do
      assert Transport.wildcard_style(Transport.Local) == {:single, "+"}
      assert Transport.wildcard_style(Transport.Mqtt) == {:single, "+"}
      assert Transport.wildcard_style(Transport.Nats) == {:single, "*"}
      assert Transport.wildcard_style(Transport.Kafka) == :exact
    end

    test "defaults to MQTT-style for a transport that does not say" do
      defmodule Minimal do
        @behaviour AshAsyncApi.Transport

        @impl true
        def child_spec(_context), do: nil
        @impl true
        def publish(_context, _address, _body, _opts), do: :ok
        @impl true
        def subscribe(_context, _filter), do: :ok
      end

      assert Transport.wildcard_style(Minimal) == {:single, "+"}
      assert Transport.delivery_scope(Minimal) == :cluster
    end
  end

  describe "delivery_scope/1" do
    test "Kafka is local, because consumer groups already deduplicate" do
      assert Transport.delivery_scope(Transport.Kafka) == :local
    end

    test "the others fan out across the cluster" do
      assert Transport.delivery_scope(Transport.Local) == :cluster
      assert Transport.delivery_scope(Transport.Mqtt) == :cluster
      assert Transport.delivery_scope(Transport.Nats) == :cluster
    end
  end

  describe "subscription filters" do
    setup do
      %{
        table: Router.__ash_async_api__(),
        server: AshAsyncApi.Domain.Info.server(Helpdesk, :cluster)
      }
    end

    test "only channels with receive operations are subscribed to", %{
      table: table,
      server: server
    } do
      filters = AshAsyncApi.Supervisor.subscription_filters(table, server)

      # ticket_commands and comment_commands have `subscribe` operations; ticket_events
      # and comment_events are publish-only.
      assert "helpdesk/tickets/commands" in filters
      assert "helpdesk/tickets/+/comments/new" in filters
      refute "helpdesk/tickets/+/events" in filters
    end

    test "the filter uses the transport's wildcard syntax", %{table: table, server: server} do
      nats_server = %{server | transport: Transport.Nats}
      kafka_server = %{server | transport: Transport.Kafka}

      assert "helpdesk/tickets/*/comments/new" in AshAsyncApi.Supervisor.subscription_filters(
               table,
               nats_server
             )

      assert "helpdesk/tickets" in AshAsyncApi.Supervisor.subscription_filters(
               table,
               kafka_server
             )
    end
  end

  describe "default encode/decode" do
    setup do
      server = AshAsyncApi.Domain.Info.server(Helpdesk, :cluster)

      %{context: Transport.Context.new(Router, Helpdesk, server)}
    end

    test "an envelope round trips through JSON", %{context: context} do
      envelope =
        AshAsyncApi.Envelope.new(
          channel: :ticket_events,
          address: "helpdesk/tickets/1/events",
          message: "ticketOpened",
          payload: %{"subject" => "Round trip"},
          correlation_id: "abc"
        )

      assert {:ok, body} = Transport.Local.encode(context, envelope)
      assert {:ok, decoded} = Transport.Local.decode(context, IO.iodata_to_binary(body))

      rebuilt = AshAsyncApi.Envelope.from_wire(decoded)

      assert rebuilt.id == envelope.id
      assert rebuilt.channel == :ticket_events
      assert rebuilt.message == "ticketOpened"
      assert rebuilt.payload == %{"subject" => "Round trip"}
      assert rebuilt.correlation_id == "abc"
    end

    test "a body that is not JSON comes back as the raw binary", %{context: context} do
      assert {:ok, "not json at all"} = Transport.Local.decode(context, "not json at all")
    end

    test "the wire form omits empty and nil fields", %{context: context} do
      envelope = AshAsyncApi.Envelope.new(channel: :ticket_events, payload: %{})

      assert {:ok, body} = Transport.Local.encode(context, envelope)
      wire = body |> IO.iodata_to_binary() |> Jason.decode!()

      refute Map.has_key?(wire, "correlationId")
      refute Map.has_key?(wire, "replyTo")
      refute Map.has_key?(wire, "headers")
    end
  end

  describe "Kafka address mapping" do
    test "the literal prefix is the topic and the parameters are the key" do
      assert Transport.Kafka.split_address(
               "helpdesk.tickets.42",
               "helpdesk.tickets.{ticket_id}"
             ) == {"helpdesk.tickets", "42"}
    end

    test "an untemplated address has no key" do
      assert Transport.Kafka.split_address("helpdesk.audit", "helpdesk.audit") ==
               {"helpdesk.audit", nil}
    end

    test "slash separated addresses work too" do
      assert Transport.Kafka.split_address(
               "helpdesk/tickets/42/events",
               "helpdesk/tickets/{id}/events"
             ) == {"helpdesk/tickets", "42/events"}
    end

    test "topic and key rejoin into the original address" do
      template = "helpdesk.tickets.{ticket_id}"
      address = "helpdesk.tickets.42"

      {topic, key} = Transport.Kafka.split_address(address, template)

      assert Transport.Kafka.join_address(topic, key, template) == address
    end

    test "joining with no key is just the topic" do
      assert Transport.Kafka.join_address("helpdesk.audit", nil) == "helpdesk.audit"
      assert Transport.Kafka.join_address("helpdesk.audit", "") == "helpdesk.audit"
    end
  end

  describe "validate_opts/2" do
    test "Kafka requires a group_id" do
      server = %AshAsyncApi.Server{name: :kafka, host: "localhost:9092", protocol: :kafka}

      assert {:error, message} = Transport.Kafka.validate_opts(server, [])
      assert message =~ "requires a :group_id"
      assert message =~ "consumer groups"
    end

    test "Kafka reports the missing client library once configured correctly" do
      server = %AshAsyncApi.Server{name: :kafka, host: "localhost:9092", protocol: :kafka}

      # :brod is an optional dependency and is not installed here.
      assert {:error, message} = Transport.Kafka.validate_opts(server, group_id: "app")
      assert message =~ ":brod library is required"
    end

    test "MQTT reports the missing client library" do
      server = %AshAsyncApi.Server{name: :mqtt, host: "localhost:1883", protocol: :mqtt}

      assert {:error, message} = Transport.Mqtt.validate_opts(server, [])
      assert message =~ ":emqtt library is required"
    end

    test "NATS reports the missing client library" do
      server = %AshAsyncApi.Server{name: :nats, host: "localhost:4222", protocol: :nats}

      assert {:error, message} = Transport.Nats.validate_opts(server, [])
      assert message =~ ":gnat library is required"
    end
  end

  describe "Context" do
    setup do
      server = AshAsyncApi.Domain.Info.server(Helpdesk, :cluster)

      %{context: Transport.Context.new(Router, Helpdesk, server, extra: :value)}
    end

    test "carries the router, domain, server and group", %{context: context} do
      assert context.router == Router
      assert context.domain == Helpdesk
      assert context.server.name == :cluster
      assert context.transport == Transport.Local
      assert context.group == AshAsyncApi.PubSub.group_name(Router)
    end

    test "merges extra options over the server's transport_opts", %{context: context} do
      assert Transport.Context.opt(context, :extra) == :value
      assert Transport.Context.opt(context, :missing, :fallback) == :fallback
    end

    test "opt!/2 explains where to set a missing option", %{context: context} do
      error = assert_raise ArgumentError, fn -> Transport.Context.opt!(context, :client_id) end

      assert Exception.message(error) =~ "requires the :client_id option"
      assert Exception.message(error) =~ "transport_opts [client_id: ...]"
    end

    test "the process name is unique per router and server", %{context: context} do
      assert Transport.Context.process_name(context) ==
               AshAsyncApi.Test.Router.Transport.Cluster
    end
  end

  describe "the Local transport" do
    test "starts no process, because AshAsyncApi.PubSub is the delivery mechanism" do
      server = AshAsyncApi.Domain.Info.server(Helpdesk, :cluster)
      context = Transport.Context.new(Router, Helpdesk, server)

      assert Transport.Local.child_spec(context) == nil
    end

    test "the router's supervision tree therefore has only the Group instance" do
      assert AshAsyncApi.Supervisor.transport_children(Router) == []
    end
  end

  describe "routing table" do
    setup do
      %{table: Router.__ash_async_api__()}
    end

    test "channel keys are the DSL names when they do not collide", %{table: table} do
      assert Enum.map(table.channels, & &1.key) |> Enum.sort() ==
               [:comment_commands, :comment_events, :ticket_commands, :ticket_events]
    end

    test "operations are indexed by resource, action and direction", %{table: table} do
      assert [operation] =
               Table.operations_for(table, Helpdesk.Ticket, :close, :send)

      assert operation.message_name == "ticketClose"
      assert Table.operations_for(table, Helpdesk.Ticket, :close, :receive) == []
    end

    test "only channels with receive operations are in the inbound index", %{table: table} do
      assert Enum.map(table.inbound, & &1.key) |> Enum.sort() ==
               [:comment_commands, :ticket_commands]
    end

    test "match_address/3 finds the channel and extracts parameters", %{table: table} do
      assert [{channel, params}] =
               Table.match_address(table, "helpdesk/tickets/7/comments/new", :cluster)

      assert channel.key == :comment_commands
      assert params == %{ticket_id: "7"}
    end

    test "match_address/3 returns nothing for an unknown address", %{table: table} do
      assert Table.match_address(table, "nope/nope", :cluster) == []
    end

    test "match_address/3 respects the server", %{table: table} do
      assert Table.match_address(table, "helpdesk/tickets/commands", :other_server) == []
    end

    test "the table is cached, so repeated reads are the same term", %{table: table} do
      assert Router.__ash_async_api__() == table
    end
  end
end
