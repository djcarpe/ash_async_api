defmodule AshAsyncApi.DslTest do
  use ExUnit.Case, async: true

  import AshAsyncApi.Test.DslHelpers

  alias AshAsyncApi.Domain.Info, as: DomainInfo
  alias AshAsyncApi.Resource.Info, as: ResourceInfo
  alias AshAsyncApi.Test.Helpdesk
  alias AshAsyncApi.Test.Helpdesk.Comment
  alias AshAsyncApi.Test.Helpdesk.Ticket

  describe "resource introspection" do
    test "reads the declared type" do
      assert ResourceInfo.type(Ticket) == "ticket"
    end

    test "defaults the type to the resource's short name" do
      defmodule Untyped do
        use Ash.Resource,
          domain: nil,
          extensions: [AshAsyncApi.Resource],
          validate_domain_inclusion?: false

        attributes do
          uuid_primary_key(:id)
        end
      end

      assert ResourceInfo.type(Untyped) == "untyped"
    end

    test "reads channels with their parameters" do
      assert [events, commands] = ResourceInfo.channels(Ticket)

      assert events.name == :ticket_events
      assert events.segments == ["helpdesk", :organization_id, "tickets", :id, "events"]
      assert [%{name: :id, description: "The id of the ticket"}] = events.parameters

      assert commands.name == :ticket_commands
      assert commands.parameters == []
    end

    test "reads operations with their directions" do
      operations = ResourceInfo.operations(Ticket)

      assert Enum.map(operations, &{&1.direction, &1.action}) == [
               {:send, :open},
               {:send, :close},
               {:send, :escalate},
               {:receive, :open}
             ]
    end

    test "hide_fields is honoured by show_field?/2" do
      assert ResourceInfo.hide_fields(Ticket) == [:internal_notes]
      refute ResourceInfo.show_field?(Ticket, :internal_notes)
      assert ResourceInfo.show_field?(Ticket, :subject)
    end

    test "async_api?/1 distinguishes extended resources" do
      assert ResourceInfo.async_api?(Ticket)
      refute ResourceInfo.async_api?(AshAsyncApi.DslTest)
      refute ResourceInfo.async_api?(:not_a_module)
    end
  end

  describe "default operation names" do
    test "operation ids default to type_action" do
      operations = ResourceInfo.operations(Ticket)

      assert Enum.find(operations, &(&1.direction == :send and &1.action == :close)).name ==
               :ticket_close
    end

    test "an action used by both directions gets the direction appended" do
      names =
        Ticket
        |> ResourceInfo.operations()
        |> Enum.filter(&(&1.action == :open))
        |> Enum.map(& &1.name)

      assert names == [:ticket_open_send, :ticket_open_receive]
    end

    test "message names default to a camelized type_action" do
      assert Enum.find(ResourceInfo.operations(Ticket), &(&1.action == :close)).message_name ==
               "ticketClose"
    end

    test "the message name ignores the direction suffix on a disambiguated operation id" do
      send_open =
        Enum.find(
          ResourceInfo.operations(Ticket),
          &(&1.action == :open and &1.direction == :send)
        )

      # The operation id needed disambiguating, but the message name must not become
      # "ticketOpenSend" — it is set explicitly here, and would default to "ticketOpen".
      assert send_open.name == :ticket_open_send
      assert send_open.message_name == "ticketOpened"
    end

    test "an explicit message_name wins over the default" do
      escalate = Enum.find(ResourceInfo.operations(Ticket), &(&1.action == :escalate))

      assert escalate.message_name == "ticketEscalated"
    end

    test "the resource and action type are stamped onto operations" do
      open = Enum.find(ResourceInfo.operations(Ticket), &(&1.direction == :send))

      assert open.resource == Ticket
      assert open.action_type == :create
    end
  end

  describe "domain introspection" do
    test "reads the info block" do
      info = DomainInfo.info(Helpdesk)

      assert info.title == "Helpdesk Events"
      assert info.version == "2.1.0"
      assert info.contact_email == "helpdesk@example.com"
      assert info.license_name == "MIT"
    end

    test "defaults the title to the domain's short name, split on case" do
      defmodule BareDomain do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        resources do
        end
      end

      assert DomainInfo.info(BareDomain).title == "Bare Domain"
      assert DomainInfo.info(BareDomain).version == "1.0.0"
    end

    test "reads servers with their transports" do
      assert [server] = DomainInfo.servers(Helpdesk)

      assert server.name == :cluster
      assert server.protocol == :erlang
      assert server.transport == AshAsyncApi.Transport.Local
    end

    test "a single server becomes the default" do
      assert DomainInfo.default_server(Helpdesk) == :cluster
    end

    test "reads domain channels and operations" do
      assert Enum.map(DomainInfo.channels(Helpdesk), & &1.name) ==
               [:comment_events, :comment_commands]

      assert [publish, subscribe] = DomainInfo.operations(Helpdesk)

      assert publish.resource == Comment
      assert publish.action == :add
      assert publish.direction == :send
      assert publish.message_name == "commentAdded"

      assert subscribe.direction == :receive
      assert subscribe.name == :comment_add_command
    end

    test "an explicitly named operation leaves its sibling's default name alone" do
      # {Comment, :add} has both a publish and a subscribe, but the subscribe named
      # itself, so the publish keeps the clean `comment_add` rather than
      # `comment_add_send`.
      assert [publish, _subscribe] = DomainInfo.operations(Helpdesk)

      assert publish.name == :comment_add
    end

    test "connected_servers/1 excludes description-only servers" do
      assert DomainInfo.connected_servers(Helpdesk) |> Enum.map(& &1.name) == [:cluster]
    end
  end

  describe "verifiers" do
    test "rejects a documented parameter the address does not contain" do
      assert_dsl_error ~r/its address does not contain/ do
        defmodule BadParameter do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key(:id)
          end

          async_api do
            channels do
              channel :events, ["tickets", "events"] do
                parameter :ticket_id
              end
            end
          end
        end
      end
    end

    test "rejects an operation bound to a nonexistent action" do
      assert_dsl_error ~r/No such action :nope/ do
        defmodule BadAction do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key(:id)
          end

          async_api do
            channels do
              channel :events, ["tickets", "events"]
            end

            operations do
              publish :nope, :events
            end
          end
        end
      end
    end

    test "rejects publishing from a read action, which emits no notifications" do
      assert_dsl_error ~r/read actions do not emit notifications/ do
        defmodule PublishFromRead do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key(:id)
          end

          actions do
            defaults([:read])
          end

          async_api do
            channels do
              channel :events, ["tickets", "events"]
            end

            operations do
              publish :read, :events
            end
          end
        end
      end
    end

    test "rejects an unknown field in payload_fields" do
      assert_dsl_error ~r/Unknown field\(s\) \[:nope\]/ do
        defmodule BadField do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key(:id)
          end

          actions do
            defaults([:create])
          end

          async_api do
            channels do
              channel :events, ["tickets", "events"]
            end

            operations do
              publish :create, :events do
                payload_fields [:id, :nope]
              end
            end
          end
        end
      end
    end

    test "rejects duplicate operation names" do
      assert_dsl_error ~r/Duplicate operation name/ do
        defmodule DuplicateNames do
          use Ash.Resource,
            domain: nil,
            extensions: [AshAsyncApi.Resource],
            validate_domain_inclusion?: false

          attributes do
            uuid_primary_key(:id)
          end

          actions do
            defaults([:create])

            create(:other)
          end

          async_api do
            channels do
              channel :events, ["tickets", "events"]
            end

            operations do
              publish :create, :events, name: :same
              publish :other, :events, name: :same
            end
          end
        end
      end
    end

    test "rejects a channel referencing an undeclared server" do
      assert_dsl_error ~r/references unknown server\(s\) \[:nope\]/ do
        defmodule BadServerRef do
          use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

          resources do
          end

          async_api do
            servers do
              server :real, "localhost:1883" do
                protocol :mqtt
              end
            end

            channels do
              channel :events, "tickets/events" do
                servers [:nope]
              end
            end
          end
        end
      end
    end

    test "rejects a default_server that does not exist" do
      assert_dsl_error ~r/does not match any declared server/ do
        defmodule BadDefaultServer do
          use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

          resources do
          end

          async_api do
            default_server :nope

            servers do
              server :real, "localhost:1883" do
                protocol :mqtt
              end

              server :other, "localhost:1884" do
                protocol :mqtt
              end
            end
          end
        end
      end
    end

    test "a transport can reject its own configuration" do
      assert_dsl_error ~r/requires a :group_id/ do
        defmodule KafkaWithoutGroup do
          use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

          resources do
          end

          async_api do
            servers do
              server :kafka, "localhost:9092" do
                protocol :kafka
                transport AshAsyncApi.Transport.Kafka
              end
            end
          end
        end
      end
    end
  end

  describe "notifier attachment" do
    test "resources with publish operations get the notifier" do
      assert AshAsyncApi.Notifier in Ash.Resource.Info.notifiers(Ticket)
    end

    test "resources whose operations live on the domain get it too" do
      # Comment declares no operations itself; its `publish` is on the domain, which the
      # resource cannot see — so the notifier is attached regardless.
      assert AshAsyncApi.Notifier in Ash.Resource.Info.notifiers(Comment)
    end

    test "publish_on_notification? false opts out" do
      defmodule ManualPublish do
        use Ash.Resource,
          domain: nil,
          extensions: [AshAsyncApi.Resource],
          validate_domain_inclusion?: false

        attributes do
          uuid_primary_key(:id)
        end

        actions do
          defaults([:create])
        end

        async_api do
          publish_on_notification? false

          channels do
            channel :events, ["tickets", "events"]
          end

          operations do
            publish :create, :events
          end
        end
      end

      refute AshAsyncApi.Notifier in Ash.Resource.Info.notifiers(ManualPublish)
    end
  end
end
