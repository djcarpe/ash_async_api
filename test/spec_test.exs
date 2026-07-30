defmodule AshAsyncApi.SpecTest do
  use ExUnit.Case, async: true

  alias AshAsyncApi.Test.Router

  setup_all do
    %{spec: AshAsyncApi.Spec.generate(Router)}
  end

  describe "document root" do
    test "declares AsyncAPI 3.0.0", %{spec: spec} do
      assert spec["asyncapi"] == "3.0.0"
    end

    test "carries the domain's id and default content type", %{spec: spec} do
      assert spec["id"] == "urn:com:example:helpdesk"
      assert spec["defaultContentType"] == "application/json"
    end

    test "the document is JSON encodable", %{spec: spec} do
      assert {:ok, json} = Jason.encode(spec)
      assert {:ok, ^spec} = Jason.decode(json)
    end
  end

  describe "info" do
    test "comes from the info block", %{spec: spec} do
      assert spec["info"]["title"] == "Helpdesk Events"
      assert spec["info"]["version"] == "2.1.0"
      assert spec["info"]["description"] == "Everything that happens to a ticket."
    end

    test "renders contact and license as objects", %{spec: spec} do
      assert spec["info"]["contact"] == %{
               "name" => "Helpdesk Team",
               "email" => "helpdesk@example.com"
             }

      assert spec["info"]["license"] == %{"name" => "MIT"}
    end

    test "renders tags as named objects", %{spec: spec} do
      assert spec["info"]["tags"] == [%{"name" => "helpdesk"}]
    end

    test "the :info option overrides derived values" do
      spec = AshAsyncApi.Spec.generate(Router, info: %{version: "9.9.9"})

      assert spec["info"]["version"] == "9.9.9"
      assert spec["info"]["title"] == "Helpdesk Events"
    end
  end

  describe "servers" do
    test "the Local transport is omitted by default, as an implementation detail", %{spec: spec} do
      refute Map.has_key?(spec, "servers")
    end

    test "include_local? surfaces it" do
      spec = AshAsyncApi.Spec.generate(Router, include_local?: true)

      assert spec["servers"]["cluster"] == %{
               "host" => "erlang-distribution",
               "protocol" => "erlang",
               "description" => "In-cluster delivery, no broker involved"
             }
    end

    test "hiding the server does not hide the channels it carries", %{spec: spec} do
      # The message shapes are the API description; they are useful with or without a
      # broker a consumer could connect to.
      assert Map.keys(spec["channels"]) |> Enum.sort() ==
               ["comment_commands", "comment_events", "ticket_commands", "ticket_events"]

      assert spec["components"]["messages"]["ticketOpened"]
    end

    test "an explicit :servers list does narrow the channels" do
      spec = AshAsyncApi.Spec.generate(Router, servers: [:nonexistent])

      refute Map.has_key?(spec, "channels")
    end
  end

  describe "channels" do
    setup do
      %{spec: AshAsyncApi.Spec.generate(Router, include_local?: true)}
    end

    test "one entry per channel with operations", %{spec: spec} do
      assert Map.keys(spec["channels"]) |> Enum.sort() ==
               ["comment_commands", "comment_events", "ticket_commands", "ticket_events"]
    end

    test "carries the templated address verbatim", %{spec: spec} do
      assert spec["channels"]["ticket_events"]["address"] ==
               "helpdesk/tickets/{ticket_id}/events"
    end

    test "every address parameter is described", %{spec: spec} do
      assert spec["channels"]["ticket_events"]["parameters"] == %{
               "ticket_id" => %{"description" => "The id of the ticket"}
             }
    end

    test "an undeclared parameter still appears, as the spec requires", %{spec: spec} do
      assert spec["channels"]["comment_events"]["parameters"] == %{
               "ticket_id" => %{
                 "description" => "The id of the ticket the comment belongs to"
               }
             }
    end

    test "a channel with no parameters has none", %{spec: spec} do
      refute Map.has_key?(spec["channels"]["ticket_commands"], "parameters")
    end

    test "messages are referenced, not inlined", %{spec: spec} do
      assert spec["channels"]["ticket_events"]["messages"] == %{
               "ticketOpened" => %{"$ref" => "#/components/messages/ticketOpened"},
               "ticketClose" => %{"$ref" => "#/components/messages/ticketClose"},
               "ticketEscalated" => %{"$ref" => "#/components/messages/ticketEscalated"}
             }
    end

    test "a channel on every server does not list them redundantly", %{spec: spec} do
      refute Map.has_key?(spec["channels"]["ticket_events"], "servers")
    end
  end

  describe "operations" do
    setup do
      %{operations: AshAsyncApi.Spec.generate(Router, include_local?: true)["operations"]}
    end

    test "publish becomes send and subscribe becomes receive", %{operations: operations} do
      assert operations["ticket_open_send"]["action"] == "send"
      assert operations["ticket_open_receive"]["action"] == "receive"
    end

    test "each operation references its channel", %{operations: operations} do
      assert operations["ticket_close"]["channel"] == %{"$ref" => "#/channels/ticket_events"}
    end

    test "each operation references its message within the channel", %{operations: operations} do
      assert operations["ticket_close"]["messages"] == [
               %{"$ref" => "#/channels/ticket_events/messages/ticketClose"}
             ]
    end

    test "the summary comes from the DSL", %{operations: operations} do
      assert operations["ticket_open_send"]["summary"] == "A ticket was opened"
    end

    test "the description falls back to the action's own description", %{operations: operations} do
      assert operations["ticket_close"]["description"] == "Close a ticket"
    end

    test "a reply_channel renders as a reply", %{operations: operations} do
      assert operations["ticket_open_receive"]["reply"] == %{
               "channel" => %{"$ref" => "#/channels/ticket_events"}
             }
    end

    test "domain-declared operations appear too", %{operations: operations} do
      assert operations["comment_add"]["action"] == "send"
      assert operations["comment_add"]["channel"] == %{"$ref" => "#/channels/comment_events"}
    end
  end

  describe "components.messages" do
    setup do
      %{
        messages:
          AshAsyncApi.Spec.generate(Router, include_local?: true)["components"]["messages"]
      }
    end

    test "one message per operation", %{messages: messages} do
      assert Map.keys(messages) |> Enum.sort() == [
               "addComment",
               "commentAdded",
               "openTicket",
               "ticketClose",
               "ticketEscalated",
               "ticketOpened"
             ]
    end

    test "each message references its payload schema", %{messages: messages} do
      assert messages["ticketOpened"]["payload"] == %{
               "$ref" => "#/components/schemas/ticketOpenedPayload"
             }
    end

    test "content type is carried through", %{messages: messages} do
      assert messages["ticketOpened"]["contentType"] == "application/json"
    end
  end

  describe "components.schemas" do
    setup do
      %{schemas: AshAsyncApi.Spec.generate(Router, include_local?: true)["components"]["schemas"]}
    end

    test "payload_fields narrows the schema to exactly those fields", %{schemas: schemas} do
      assert Map.keys(schemas["ticketOpenedPayload"]["properties"]) |> Enum.sort() ==
               ["id", "opened_at", "priority", "status", "subject"]
    end

    test "an unnarrowed payload uses the public attributes minus hidden ones", %{schemas: schemas} do
      properties = Map.keys(schemas["ticketClosePayload"]["properties"])

      assert "subject" in properties
      assert "estimated_cost" in properties
      refute "internal_notes" in properties
    end

    test "Ash types map onto JSON Schema types and formats", %{schemas: schemas} do
      properties = schemas["ticketClosePayload"]["properties"]

      assert properties["id"] == %{"type" => "string", "format" => "uuid"}
      assert properties["opened_at"] == %{"type" => ["string", "null"], "format" => "date-time"}

      assert properties["estimated_cost"] == %{
               "type" => ["string", "null"],
               "format" => "decimal"
             }
    end

    test "constraints become schema constraints", %{schemas: schemas} do
      properties = schemas["ticketClosePayload"]["properties"]

      assert properties["subject"] == %{"type" => "string", "maxLength" => 200}
      assert properties["status"] == %{"type" => "string", "enum" => ["open", "closed"]}
    end

    test "non-nullable fields are required", %{schemas: schemas} do
      required = schemas["ticketOpenedPayload"]["required"]

      assert "subject" in required
      assert "status" in required
      refute "opened_at" in required
    end

    test "a receive payload is derived from the action's inputs", %{schemas: schemas} do
      properties = schemas["openTicketPayload"]["properties"]

      assert Map.keys(properties) |> Enum.sort() == ["body", "priority", "subject"]
      assert schemas["openTicketPayload"]["required"] == ["subject"]
    end
  end

  describe "to_json/2 and to_yaml/2" do
    test "produce parseable output" do
      json = AshAsyncApi.Spec.to_json(Router)

      assert {:ok, decoded} = Jason.decode(json)
      assert decoded["asyncapi"] == "3.0.0"
    end

    test "YAML round trips back to the same document" do
      yaml = AshAsyncApi.Spec.to_yaml(Router)

      assert {:ok, decoded} = YamlElixir.read_from_string(yaml)
      assert decoded["asyncapi"] == "3.0.0"
      assert decoded["info"]["title"] == "Helpdesk Events"
    end
  end
end
