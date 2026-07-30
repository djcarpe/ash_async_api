defmodule Helpdesk.Demo do
  @moduledoc """
  Functions the demo script calls over `rpc`, so you can drive the system from outside.
  """

  require Logger

  alias Helpdesk.Support.Ticket

  @doc """
  Open a ticket. The notifier publishes `ticketOpened` to MQTT and to every subscriber in
  the cluster, with no publishing code here at all.
  """
  def open(subject, opts \\ []) do
    Ticket
    |> Ash.Changeset.for_create(:open, %{
      subject: subject,
      priority: Keyword.get(opts, :priority, :normal),
      internal_notes: "never leaves the database"
    })
    |> Ash.create!()
  end

  @doc "Close the most recently opened ticket, publishing `ticketClosed`."
  def close_latest do
    case latest() do
      nil -> {:error, :no_tickets}
      ticket -> ticket |> Ash.Changeset.for_update(:close, %{}) |> Ash.update!()
    end
  end

  @doc """
  Escalate the most recent ticket.

  The `escalate` operation has a `filter`, so the message is only published when the
  priority actually became `:urgent`.
  """
  def escalate_latest do
    case latest() do
      nil -> {:error, :no_tickets}
      ticket -> ticket |> Ash.Changeset.for_update(:escalate, %{}) |> Ash.update!()
    end
  end

  @doc "Tickets in *this* node's ETS table, newest first."
  def tickets do
    Ticket |> Ash.read!() |> Enum.sort_by(& &1.opened_at, {:desc, DateTime})
  end

  @doc "What this node's audit log has seen."
  def audit do
    Helpdesk.AuditLog.entries()
    |> Enum.map(&%{message: &1.message, address: &1.address, payload: &1.payload})
  end

  @doc """
  Watch one specific ticket, rather than all of them.

  Subscribing to the concrete address means this process is never woken for other
  tickets — the reason channel addresses are templated in the first place.
  """
  def watch(ticket_id, timeout \\ 10_000) do
    address = "helpdesk/tickets/#{ticket_id}/events"
    :ok = AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, address)

    Logger.info("[watch] listening on #{address}")

    receive do
      {:ash_async_api, envelope} -> {:ok, envelope.message, envelope.payload}
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc "A summary of what this node currently knows, for the demo script."
  def summary do
    %{
      node: node(),
      cluster: Node.list(),
      group_nodes: AshAsyncApi.PubSub.nodes(Helpdesk.AsyncApiRouter),
      tickets: Enum.map(tickets(), &%{id: &1.id, subject: &1.subject, status: &1.status}),
      audit_count: length(Helpdesk.AuditLog.entries())
    }
  end

  defp latest, do: tickets() |> List.first()
end
