defmodule AshAsyncApi.SpecialSegmentsTest do
  @moduledoc """
  The special address segments — `:_domain`, `:_resource`, `:_event`, `:_pkey` — and
  the per-resource channels one fragment-declared channel resolves into.
  """

  use AshAsyncApi.RouterCase, async: false

  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Test.Crm.Tag
  alias AshAsyncApi.Test.CrmRouter

  describe "channel resolution" do
    test "one fragment channel becomes a distinct channel per resource" do
      table = CrmRouter.__ash_async_api__()

      lead_events = Table.channel(table, :lead_events)
      tag_events = Table.channel(table, :tag_events)

      assert lead_events.address == "crm.lead.{event}.{id}"
      assert lead_events.resource == AshAsyncApi.Test.Crm.Lead

      # Tag's primary key is composite, so it cannot be addressed by one field.
      assert tag_events.address == "crm.tag.{event}.{pkey}"
      assert tag_events.resource == Tag
    end

    test "a resource without a primary key gets a placeholder token" do
      defmodule Keyless do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          type "keyless"

          channels do
            channel :events, [:_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          attribute :label, :string, public?: true
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule KeylessDomain do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        resources do
          resource Keyless
        end
      end

      table = Table.build(:keyless_router, [KeylessDomain])

      assert [channel] = table.channels
      # An empty token would be an illegal NATS subject; `_` keeps the depth stable.
      assert channel.address == "keyless/{event}/_"
    end

    test "segment_naming :camel renders the domain and resource segments lowerCamel" do
      defmodule CamelWorkOrder do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule CamelServiceDesk do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        async_api do
          segment_naming :camel
        end

        resources do
          resource CamelWorkOrder
        end
      end

      table = Table.build(:camel_router, [CamelServiceDesk])

      assert [channel] = table.channels
      # Domain "camel_service_desk" and resource "camel_work_order", re-cased.
      assert channel.address == "camelServiceDesk/camelWorkOrder/{event}/{id}"
    end

    test "a resource-level segment_naming overrides the domain's for its channels" do
      defmodule PlainReceipt do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule CamelSalesOrder do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          segment_naming :camel

          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule MixedBackOffice do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        resources do
          resource PlainReceipt
          resource CamelSalesOrder
        end
      end

      table = Table.build(:mixed_router, [MixedBackOffice])
      addresses = table.channels |> Enum.map(& &1.address) |> Enum.sort()

      # The overriding resource camelizes its whole address — domain segment
      # included — while its sibling keeps the domain's default snake.
      assert addresses == [
               "mixedBackOffice/camelSalesOrder/{event}/{id}",
               "mixed_back_office/plain_receipt/{event}/{id}"
             ]
    end

    test "segment_naming :pascal upper-camelizes the segments" do
      defmodule PascalWorkItem do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule PascalFrontDesk do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        async_api do
          segment_naming :pascal
        end

        resources do
          resource PascalWorkItem
        end
      end

      table = Table.build(:pascal_router, [PascalFrontDesk])

      assert [channel] = table.channels
      assert channel.address == "PascalFrontDesk/PascalWorkItem/{event}/{id}"
    end

    test "application config supplies the naming when the DSL does not" do
      defmodule ConfiguredGadget do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      # No segment_naming in the DSL; the otp_app's config decides.
      defmodule ConfiguredWorkshop do
        use Ash.Domain,
          otp_app: :segment_naming_config_test,
          extensions: [AshAsyncApi.Domain],
          validate_config_inclusion?: false

        resources do
          resource ConfiguredGadget
        end
      end

      Application.put_env(:segment_naming_config_test, :ash_async_api_segment_naming, :pascal)

      on_exit(fn ->
        Application.delete_env(:segment_naming_config_test, :ash_async_api_segment_naming)
      end)

      table = Table.build(:configured_router, [ConfiguredWorkshop])

      assert [channel] = table.channels
      assert channel.address == "ConfiguredWorkshop/ConfiguredGadget/{event}/{id}"
    end

    test "DSL-level segment_naming beats application config" do
      defmodule LoudGadget do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule LoudWorkshop do
        use Ash.Domain,
          otp_app: :segment_naming_precedence_test,
          extensions: [AshAsyncApi.Domain],
          validate_config_inclusion?: false

        async_api do
          segment_naming :camel
        end

        resources do
          resource LoudGadget
        end
      end

      Application.put_env(:segment_naming_precedence_test, :ash_async_api_segment_naming, :pascal)

      on_exit(fn ->
        Application.delete_env(:segment_naming_precedence_test, :ash_async_api_segment_naming)
      end)

      table = Table.build(:loud_router, [LoudWorkshop])

      assert [channel] = table.channels
      assert channel.address == "loudWorkshop/loudGadget/{event}/{id}"
    end

    test "segment_naming as a function takes full control of both segments" do
      defmodule FunOrder do
        use Ash.Resource,
          domain: nil,
          validate_domain_inclusion?: false,
          data_layer: Ash.DataLayer.Ets,
          extensions: [AshAsyncApi.Resource]

        ets do
          private? true
        end

        async_api do
          channels do
            channel :events, [:_domain, :_resource, :_event, :_pkey]
          end

          operations do
            publish_all :create, :events
          end
        end

        attributes do
          uuid_primary_key :id
        end

        actions do
          defaults [:read, create: :*]
        end
      end

      defmodule FunDesk do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        async_api do
          segment_naming fn module ->
            "v2-" <> (module |> Module.split() |> List.last() |> String.downcase())
          end
        end

        resources do
          resource FunOrder
        end
      end

      table = Table.build(:fun_router, [FunDesk])

      assert [channel] = table.channels
      assert channel.address == "v2-fundesk/v2-funorder/{event}/{id}"
    end

    test "a domain-scoped channel cannot use resource-specific segments" do
      defmodule BadDomain do
        use Ash.Domain, extensions: [AshAsyncApi.Domain], validate_config_inclusion?: false

        async_api do
          channels do
            channel :events, [:_resource, "events"]
          end
        end

        resources do
        end
      end

      assert_raise ArgumentError, ~r/only resolves on a resource-scoped channel/, fn ->
        Table.build(:bad_router, [BadDomain])
      end
    end
  end

  describe "publishing" do
    setup do
      start_router!(CrmRouter)
      :ok
    end

    test "a composite primary key becomes one joined address token" do
      CrmRouter.subscribe(:tag_events)

      Tag
      |> Ash.Changeset.for_create(:create, %{namespace: "priority", name: "high"})
      |> Ash.create!()

      envelope = assert_message(%{channel: :tag_events})
      assert envelope.address == "crm.tag.created.priority-high"
      assert envelope.params.pkey == "priority-high"
    end

    test "values that would break the address are flattened" do
      CrmRouter.subscribe(:tag_events)

      # The delimiter in a value would change the address depth; a wildcard could turn
      # a concrete subject into a filter. Both are flattened to `_`.
      Tag
      |> Ash.Changeset.for_create(:create, %{namespace: "a.b", name: "x y*"})
      |> Ash.create!()

      envelope = assert_message(%{channel: :tag_events})
      assert envelope.address == "crm.tag.created.a_b-x_y_"
    end
  end

  describe "the generated document" do
    test "lists one concrete channel per resource" do
      spec = CrmRouter.spec()

      assert %{"address" => "crm.lead.{event}.{id}"} = spec["channels"]["lead_events"]
      assert %{"address" => "crm.tag.{event}.{pkey}"} = spec["channels"]["tag_events"]

      assert Map.has_key?(spec["channels"]["lead_events"]["parameters"], "event")
      assert Map.has_key?(spec["channels"]["lead_events"]["parameters"], "id")
    end

    test "lists the expanded operations" do
      spec = CrmRouter.spec()

      assert Map.has_key?(spec["operations"], "lead_create")
      assert Map.has_key?(spec["operations"], "lead_import")
      assert Map.has_key?(spec["operations"], "lead_qualify")
      assert Map.has_key?(spec["operations"], "tag_create")
    end
  end
end
