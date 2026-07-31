# Getting Started with AshAsyncApi

This guide builds a small helpdesk that publishes ticket events and accepts ticket
commands. It starts with no broker at all, which is the fastest way to see the whole
system work, and adds MQTT at the end.

## Install

```elixir
# mix.exs
def deps do
  [
    {:ash, "~> 3.0"},
    {:ash_async_api, "~> 0.1.0"}
  ]
end
```

## 1. Add the extension to your resource

```elixir
defmodule Helpdesk.Support.Ticket do
  use Ash.Resource,
    domain: Helpdesk.Support,
    data_layer: AshPostgres.DataLayer,
    extensions: [AshAsyncApi.Resource]

  async_api do
    type "ticket"
  end

  attributes do
    uuid_primary_key :id

    attribute :subject, :string, allow_nil?: false, public?: true

    attribute :status, :atom do
      allow_nil? false
      public? true
      default :open
      constraints one_of: [:open, :closed]
    end
  end

  actions do
    defaults [:read]

    create :open do
      accept [:subject]
    end

    update :close do
      accept []
      change set_attribute(:status, :closed)
    end
  end
end
```

`type` names the resource in the generated document. It defaults to the resource's short
name in snake case, so you can leave it out.

## 2. Declare a channel

A channel is where messages go — an MQTT topic, a NATS subject, a Kafka topic. Its address
is a **list of segments**:

```elixir
async_api do
  type "ticket"

  channels do
    channel :ticket_events, ["helpdesk", "tickets", :id, "events"] do
      description "Lifecycle events for a single ticket"
    end
  end
end
```

Note what is missing: the delimiter. You never write it, because it is not a property of
your API — it belongs to the bus. The same declaration produces

    helpdesk/tickets/<id>/events     on MQTT
    helpdesk.tickets.<id>.events     on NATS
    helpdesk:tickets:<id>:events     on Redis

Move the channel to a different broker and the address follows, along with the subscription
wildcard (`+` on MQTT, `*` on NATS). See [Transports](../topics/transports.md#delimiters).

A segment is a literal string, a field name, or a relationship path. Anything that is not a
literal becomes a **parameter**:

```elixir
# tenant, ticket, and the comment itself, all in one address
channel :comment_events,
        ["helpdesk", [:ticket, :organization_id], "tickets", [:ticket, :id], "comments", :id]
```

`[:ticket, :organization_id]` walks the relationship, and the parameter is named after the
path — `ticket_organization_id`. Use `{:name, path}` if you want to call it something else.

Rich addresses are worth the small extra effort: they let a consumer subscribe to one
ticket, or one tenant, rather than to everything, and they give the generated document
enough information for tooling to construct addresses itself.

## 3. Declare operations

An operation binds an action to a channel. The two directions are named from your
application's point of view, matching AsyncAPI 3.0:

```elixir
operations do
  # We send: running the action emits a message.
  publish :open, :ticket_events do
    message_name "ticketOpened"
  end

  publish :close, :ticket_events

  # We receive: a message arriving runs the action.
  subscribe :open, :ticket_commands
end
```

`publish` renders as `action: send` in the document, `subscribe` as `action: receive`.

Operation ids and message names are derived when you do not supply them —
`publish :close` becomes operation `ticket_close` carrying message `ticketClose`.

## 4. Add the domain extension

The domain holds the document metadata and the servers:

```elixir
defmodule Helpdesk.Support do
  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    info do
      title "Helpdesk Events"
      version "1.0.0"
      description "Everything that happens to a ticket."
    end

    servers do
      # Start with no broker: delivery over the Erlang cluster.
      server :cluster, "erlang-distribution" do
        protocol :erlang
        transport AshAsyncApi.Transport.Local
      end
    end
  end

  resources do
    resource Helpdesk.Support.Ticket
  end
end
```

## 5. Add a router

```elixir
defmodule Helpdesk.AsyncApiRouter do
  use AshAsyncApi.Router, domains: [Helpdesk.Support]
end
```

Put it in your supervision tree:

```elixir
# lib/helpdesk/application.ex
children = [
  Helpdesk.Repo,
  Helpdesk.AsyncApiRouter,
  HelpdeskWeb.Endpoint
]
```

The router owns the `Group` instance that fans messages out across the cluster and
supervises your transports. Nothing works without it in the tree, so if messages seem to
vanish, check that first — `AshAsyncApi.PubSub.running?/1` will tell you.

Tell the notifier which routers to publish through:

```elixir
# config/config.exs
config :helpdesk, ash_async_api_routers: [Helpdesk.AsyncApiRouter]
```

AshAsyncApi can find them by scanning modules, but that costs a code path scan on first
use. Configure it.

## 6. Try it

```elixir
iex> AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, :ticket_events)
:ok

iex> ticket =
...>   Helpdesk.Support.Ticket
...>   |> Ash.Changeset.for_create(:open, %{subject: "Printer on fire"})
...>   |> Ash.create!()

iex> flush()
{:ash_async_api,
 %AshAsyncApi.Envelope{
   channel: :ticket_events,
   address: "helpdesk/tickets/91c1.../events",
   message: "ticketOpened",
   payload: %{id: "91c1...", subject: "Printer on fire", status: "open"}
 }}
```

No configuration beyond the operation. The notifier saw the create, matched it to the
`publish :open` operation, filled in the address from the record, and fanned the envelope
out.

Note that `status` arrived as the string `"open"`, not the atom `:open`. Payloads are
normalized to JSON-safe values so that a subscriber sees the same thing whether the
message came from the local publisher or from a broker.

## 7. Subscribe to one ticket

```elixir
AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, "helpdesk/tickets/#{ticket.id}/events")
```

Now this process is woken only for that ticket. This is the pattern for a LiveView showing
one record: subscribe to its address, then load current state.

```elixir
def mount(%{"id" => id}, _session, socket) do
  if connected?(socket) do
    AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, "helpdesk/tickets/#{id}/events")
  end

  {:ok, assign(socket, ticket: Ash.get!(Helpdesk.Support.Ticket, id))}
end

def handle_info({:ash_async_api, envelope}, socket) do
  {:noreply, assign(socket, ticket: %{socket.assigns.ticket | status: envelope.payload.status})}
end
```

Subscribe *before* loading, not after. `Group` is eventually consistent, and loading first
leaves a window where a change can be missed.

## 8. Generate the document

```elixir
iex> Helpdesk.AsyncApiRouter.spec_json() |> IO.puts()
```

or

```sh
mix ash_async_api.spec --router Helpdesk.AsyncApiRouter --output asyncapi.json
```

The payload schemas come from your attributes, including types, constraints and
nullability. `mix ash_async_api.spec --check` fails when the checked-in file is stale,
which is worth putting in CI.

## 9. Point it at a real broker

Swap the server, and nothing above the transport changes:

```elixir
# mix.exs
{:emqtt, "~> 1.13"}
```

```elixir
servers do
  server :mqtt, "broker.example.com:1883" do
    protocol :mqtt
    protocol_version "5"
    transport AshAsyncApi.Transport.Mqtt
    transport_opts [
      clientid: "helpdesk",
      username: "helpdesk",
      password: {:system, "MQTT_PASSWORD"}
    ]
  end
end
```

`["helpdesk", "tickets", :id, "events"]` becomes the MQTT topic
`helpdesk/tickets/<id>/events` on the way out, and the subscription filter
`helpdesk/tickets/+/events` on the way in. Your resource definitions are unchanged, and so
is every `AshAsyncApi.subscribe/2` call.

Had you pointed it at NATS instead, the very same declaration would have produced
`helpdesk.tickets.<id>.events` and subscribed with `helpdesk.tickets.*.events`.

## 10. Handle inbound messages

Add a command channel and a `subscribe` operation:

```elixir
channels do
  channel :ticket_commands, ["helpdesk", "tickets", "commands"]
end

operations do
  subscribe :open, :ticket_commands do
    message_name "openTicket"
  end
end
```

The transport now subscribes to `helpdesk/tickets/commands` on startup. A message like

```json
{"message": "openTicket", "payload": {"subject": "From another service"}}
```

runs `Ticket.open`. A bare `{"subject": "..."}` works too, for producers that know nothing
about AshAsyncApi.

The payload is narrowed to what the action accepts before it is passed in, so a message
cannot set `status` on an action that only accepts `subject`.

### One important default

By default a router **ignores messages it published itself**, so a channel used for both
publishing and subscribing does not loop. This means you cannot use a broker round trip to
call your own action — and you should not want to: within one application, call the action.
The `subscribe` side is for messages from somewhere else.

To exercise the receive path in a test, start the router with
`ignore_own_messages?: false`.

## Where to go next

- [What is AshAsyncApi?](../topics/what-is-ash-async-api.md) — the design and its trade-offs
- [Transports](../topics/transports.md) — broker specifics, and writing your own
- [`AshAsyncApi.Resource` DSL](../dsls/DSL-AshAsyncApi.Resource.md)
- [`AshAsyncApi.Domain` DSL](../dsls/DSL-AshAsyncApi.Domain.md)
