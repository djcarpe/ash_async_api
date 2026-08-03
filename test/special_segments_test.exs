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
