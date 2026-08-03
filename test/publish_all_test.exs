defmodule AshAsyncApi.PublishAllTest do
  @moduledoc """
  `publish_all` — one declaration covering every action of a type — expanded into
  concrete per-action operations when the routing table is built.
  """

  use AshAsyncApi.RouterCase, async: false

  alias AshAsyncApi.Router.Table
  alias AshAsyncApi.Test.Crm.Lead
  alias AshAsyncApi.Test.CrmRouter

  describe "expansion" do
    test "covers every action of the type, including custom-named ones" do
      table = CrmRouter.__ash_async_api__()

      assert [create] = Table.operations_for(table, Lead, :create, :send)
      assert [import_op] = Table.operations_for(table, Lead, :import, :send)

      assert create.name == :lead_create
      assert import_op.name == :lead_import
      assert create.message_name == "leadCreate"
      assert import_op.message_name == "leadImport"
      assert create.event_verb == "created"
      assert import_op.event_verb == "created"
    end

    test "each action type gets its own verb" do
      table = CrmRouter.__ash_async_api__()

      assert [update] = Table.operations_for(table, Lead, :update, :send)
      assert [destroy] = Table.operations_for(table, Lead, :destroy, :send)

      assert update.event_verb == "updated"
      assert destroy.event_verb == "destroyed"
    end

    test "an explicit publish on the same channel wins over publish_all" do
      table = CrmRouter.__ash_async_api__()

      # :qualify is an update action with its own `publish` — expanding the
      # `publish_all :update` over it would publish the same event twice.
      assert [qualify] = Table.operations_for(table, Lead, :qualify, :send)
      assert qualify.name == :lead_qualify
      assert qualify.event_verb == "qualified"
    end

    test "expanded operations carry the publish_all's options" do
      table = CrmRouter.__ash_async_api__()

      assert [import_op] = Table.operations_for(table, Lead, :import, :send)
      assert is_function(import_op.operation.headers, 1)
    end
  end

  describe "publishing" do
    setup do
      start_router!(CrmRouter)
      :ok
    end

    test "a custom-named create publishes with the created verb" do
      CrmRouter.subscribe(:lead_events)

      lead =
        Lead
        |> Ash.Changeset.for_create(:import, %{name: "Ada"})
        |> Ash.create!()

      envelope = assert_message(%{channel: :lead_events})
      assert envelope.address == "crm.lead.created.#{lead.id}"
      assert envelope.payload[:name] == "Ada"
      assert envelope.headers["service"] == "crm"
    end

    test "update and destroy publish their own verbs" do
      CrmRouter.subscribe(:lead_events)

      lead =
        Lead
        |> Ash.Changeset.for_create(:create, %{name: "Grace"})
        |> Ash.create!()

      assert_message(%{address: "crm.lead.created." <> _})

      lead
      |> Ash.Changeset.for_update(:update, %{name: "Grace H"})
      |> Ash.update!()

      assert_message(%{address: "crm.lead.updated." <> _})

      Ash.destroy!(lead)

      envelope = assert_message(%{address: "crm.lead.destroyed." <> _})
      assert envelope.address == "crm.lead.destroyed.#{lead.id}"
    end

    test "an action with an explicit publish uses its event_name" do
      CrmRouter.subscribe(:lead_events)

      lead =
        Lead
        |> Ash.Changeset.for_create(:create, %{name: "Joan"})
        |> Ash.create!()

      assert_message(%{address: "crm.lead.created." <> _})

      lead
      |> Ash.Changeset.for_update(:qualify)
      |> Ash.update!()

      envelope = assert_message(%{address: "crm.lead.qualified." <> _})
      assert envelope.address == "crm.lead.qualified.#{lead.id}"
      refute_message()
    end
  end
end
