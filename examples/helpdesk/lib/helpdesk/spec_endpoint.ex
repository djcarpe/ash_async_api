defmodule Helpdesk.SpecEndpoint do
  @moduledoc """
  Serves the generated AsyncAPI document, plus a little status page.

  Publishing the document at a well-known URL is a genuinely useful pattern: the
  [AsyncAPI toolchain](https://www.asyncapi.com/tools) can point straight at it, and it can
  never drift from the running code because it is generated from the same declarations.
  """

  use Plug.Router

  plug :match
  plug :dispatch

  get "/asyncapi.json" do
    send_resp(conn, 200, AshAsyncApi.Spec.to_json(Helpdesk.AsyncApiRouter))
    |> halt()
  end

  get "/asyncapi.yaml" do
    conn
    |> put_resp_content_type("application/yaml")
    |> send_resp(200, AshAsyncApi.Spec.to_yaml(Helpdesk.AsyncApiRouter))
    |> halt()
  end

  get "/status" do
    router = Helpdesk.AsyncApiRouter

    status = %{
      node: to_string(node()),
      cluster: Enum.map(Node.list(), &to_string/1),
      pubsub_running: AshAsyncApi.PubSub.running?(router),
      pubsub_nodes: router |> AshAsyncApi.PubSub.nodes() |> Enum.map(&to_string/1),
      subscribers: %{
        ticket_events: AshAsyncApi.subscriber_count(router, :ticket_events),
        ticket_commands: AshAsyncApi.subscriber_count(router, :ticket_commands)
      },
      tickets: Helpdesk.Support.Ticket |> Ash.read!() |> length(),
      audit_entries: length(Helpdesk.AuditLog.entries())
    }

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(status, pretty: true))
    |> halt()
  end

  match _ do
    send_resp(conn, 404, "try /asyncapi.json, /asyncapi.yaml or /status\n")
  end
end
