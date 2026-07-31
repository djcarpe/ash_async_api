defmodule AshAsyncApi.DelimiterTest do
  @moduledoc """
  The delimiter belongs to the bus, not to the channel: one segment list should come out as
  `helpdesk/tickets/1` on MQTT and `helpdesk.tickets.1` on NATS.
  """

  use ExUnit.Case, async: true

  import AshAsyncApi.Test.DslHelpers

  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Server

  describe "protocol defaults" do
    test "hierarchical protocols use /" do
      for protocol <- [:mqtt, :mqtts, :ws, :wss, :http, :https] do
        assert Server.default_delimiter_for(protocol) == "/"
      end
    end

    test "subject and routing-key protocols use ." do
      for protocol <- [:nats, :kafka, :amqp, :amqp1, :pulsar, :googlepubsub, :sns, :sqs] do
        assert Server.default_delimiter_for(protocol) == "."
      end
    end

    test "redis uses :" do
      assert Server.default_delimiter_for(:redis) == ":"
    end

    test "an unknown protocol falls back to /" do
      assert Server.default_delimiter_for(:something_new) == "/"
      assert Server.default_delimiter_for(nil) == "/"
    end

    test "a string protocol resolves like the atom" do
      assert Server.default_delimiter_for("nats") == "."
    end
  end

  describe "Server.delimiter/1" do
    test "comes from the protocol" do
      assert Server.delimiter(%Server{protocol: :nats}) == "."
      assert Server.delimiter(%Server{protocol: :mqtt}) == "/"
    end

    test "an explicit delimiter wins" do
      assert Server.delimiter(%Server{protocol: :nats, delimiter: "/"}) == "/"
    end

    test "a transport may declare one, overriding the protocol" do
      defmodule ColonTransport do
        use AshAsyncApi.Transport

        @impl true
        def default_delimiter, do: ":"
        @impl true
        def child_spec(_context), do: nil
        @impl true
        def publish(_context, _address, _body, _opts), do: :ok
        @impl true
        def subscribe(_context, _filter), do: :ok
      end

      assert Server.delimiter(%Server{protocol: :mqtt, transport: ColonTransport}) == ":"
    end

    test "the built-in transports do not override their protocols" do
      # They have nothing to add: the protocol registry already knows.
      for transport <- [
            AshAsyncApi.Transport.Mqtt,
            AshAsyncApi.Transport.Nats,
            AshAsyncApi.Transport.Kafka,
            AshAsyncApi.Transport.Local
          ] do
        refute function_exported?(transport, :default_delimiter, 0)
      end
    end
  end

  describe "one declaration, many buses" do
    defmodule MultiBus do
      @moduledoc false
      use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

      async_api do
        servers do
          server :mqtt, "localhost:1883" do
            protocol :mqtt
          end

          server :nats, "localhost:4222" do
            protocol :nats
          end

          server :redis, "localhost:6379" do
            protocol :redis
          end
        end

        channels do
          channel :on_mqtt, ["helpdesk", "tickets", :id, "events"] do
            servers [:mqtt]
          end

          channel :on_nats, ["helpdesk", "tickets", :id, "events"] do
            servers [:nats]
          end

          channel :on_redis, ["helpdesk", "tickets", :id, "events"] do
            servers [:redis]
          end

          channel :overridden, ["helpdesk", "tickets", :id] do
            servers [:nats]
            delimiter "/"
          end
        end
      end

      resources do
      end
    end

    defmodule MultiBusRouter do
      @moduledoc false
      use AshAsyncApi.Router, domains: [MultiBus]
    end

    setup do
      %{table: MultiBusRouter.__ash_async_api__()}
    end

    test "the same segments take each bus's shape", %{table: table} do
      assert Table.channel(table, :on_mqtt).address == "helpdesk/tickets/{id}/events"
      assert Table.channel(table, :on_nats).address == "helpdesk.tickets.{id}.events"
      assert Table.channel(table, :on_redis).address == "helpdesk:tickets:{id}:events"
    end

    test "the resolved delimiter is recorded on the channel", %{table: table} do
      assert Table.channel(table, :on_mqtt).delimiter == "/"
      assert Table.channel(table, :on_nats).delimiter == "."
      assert Table.channel(table, :on_redis).delimiter == ":"
    end

    test "a channel may override its bus", %{table: table} do
      assert Table.channel(table, :overridden).delimiter == "/"
      assert Table.channel(table, :overridden).address == "helpdesk/tickets/{id}"
    end

    test "matching an inbound address uses the channel's own delimiter", %{table: table} do
      mqtt = Table.channel(table, :on_mqtt)
      nats = Table.channel(table, :on_nats)

      assert {:ok, %{id: "42"}} =
               AshAsyncApi.Address.match(mqtt.compiled, "helpdesk/tickets/42/events")

      assert :error = AshAsyncApi.Address.match(mqtt.compiled, "helpdesk.tickets.42.events")

      assert {:ok, %{id: "42"}} =
               AshAsyncApi.Address.match(nats.compiled, "helpdesk.tickets.42.events")
    end

    test "subscription filters follow the delimiter and the broker's wildcard", %{table: table} do
      nats_server = %Server{
        name: :nats,
        host: "h",
        protocol: :nats,
        transport: AshAsyncApi.Transport.Nats
      }

      mqtt_server = %Server{
        name: :mqtt,
        host: "h",
        protocol: :mqtt,
        transport: AshAsyncApi.Transport.Mqtt
      }

      # `subscription_filters/2` only covers channels with receive operations, so exercise
      # the translation directly against the compiled addresses.
      assert AshAsyncApi.Address.to_filter(
               Table.channel(table, :on_nats).compiled,
               AshAsyncApi.Transport.wildcard_style(nats_server.transport)
             ) == "helpdesk.tickets.*.events"

      assert AshAsyncApi.Address.to_filter(
               Table.channel(table, :on_mqtt).compiled,
               AshAsyncApi.Transport.wildcard_style(mqtt_server.transport)
             ) == "helpdesk/tickets/+/events"
    end
  end

  describe "conflicts" do
    defmodule Conflicting do
      @moduledoc false
      use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

      async_api do
        servers do
          server :mqtt, "localhost:1883" do
            protocol :mqtt
          end

          server :nats, "localhost:4222" do
            protocol :nats
          end
        end

        channels do
          channel :both, ["helpdesk", :id] do
            servers [:mqtt, :nats]
          end
        end
      end

      resources do
      end
    end

    defmodule ConflictingRouter do
      @moduledoc false
      use AshAsyncApi.Router, domains: [Conflicting]
    end

    test "a channel spanning buses that disagree is refused, with both options spelled out" do
      error =
        assert_raise AshAsyncApi.Error.DelimiterConflict, fn ->
          ConflictingRouter.__ash_async_api__()
        end

      message = Exception.message(error)

      assert message =~ "join address segments differently"
      assert message =~ "delimiter"
      assert message =~ "default_delimiter"
    end

    defmodule Settled do
      @moduledoc false
      use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

      async_api do
        default_delimiter(".")

        servers do
          server :mqtt, "localhost:1883" do
            protocol :mqtt
          end

          server :nats, "localhost:4222" do
            protocol :nats
          end
        end

        channels do
          channel :both, ["helpdesk", :id] do
            servers [:mqtt, :nats]
          end
        end
      end

      resources do
      end
    end

    defmodule SettledRouter do
      @moduledoc false
      use AshAsyncApi.Router, domains: [Settled]
    end

    test "a domain-wide default settles it" do
      table = SettledRouter.__ash_async_api__()

      assert Table.channel(table, :both).address == "helpdesk.{id}"
    end
  end

  describe "compile-time checks on segments" do
    test "rejects a field that does not exist" do
      assert_dsl_error ~r/is not a field on this resource/ do
        defmodule BadFieldSegment do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key :id
          end

          async_api do
            channels do
              channel :events, ["tickets", :nope]
            end
          end
        end
      end
    end

    test "rejects a relationship that does not exist" do
      assert_dsl_error ~r/is not a relationship on this resource/ do
        defmodule BadRelSegment do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key :id
          end

          async_api do
            channels do
              channel :events, ["tickets", [:nope, :id]]
            end
          end
        end
      end
    end

    test "rejects a field that does not exist on the far side of a relationship" do
      assert_dsl_error ~r/does not resolve on/ do
        defmodule BadNestedSegment do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key :id
          end

          relationships do
            belongs_to :ticket, AshAsyncApi.Test.Helpdesk.Ticket do
              public? true
            end
          end

          async_api do
            channels do
              channel :events, ["tickets", [:ticket, :nope]]
            end
          end
        end
      end
    end

    test "rejects a segment that is not a legal shape" do
      assert_dsl_error ~r/Invalid address segment 42/ do
        defmodule BadShapeSegment do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key :id
          end

          async_api do
            channels do
              channel :events, ["tickets", 42]
            end
          end
        end
      end
    end
  end
end
