defmodule Helpdesk.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      Helpdesk.Cluster,

      # The router must come before anything that subscribes: it owns the Group instance,
      # and subscribing before it is running would fail.
      Helpdesk.AsyncApiRouter,
      Helpdesk.AuditLog,
      {Bandit, plug: Helpdesk.SpecEndpoint, port: port()}
    ]

    Logger.info("[helpdesk] starting #{node()} — spec on http://localhost:#{port()}/asyncapi.json")

    Supervisor.start_link(children, strategy: :one_for_one, name: Helpdesk.Supervisor)
  end

  defp port, do: String.to_integer(System.get_env("PORT", "4000"))
end
