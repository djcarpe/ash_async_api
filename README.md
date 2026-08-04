# AshAsyncApi

The extension for building [AsyncAPI 3.0](https://www.asyncapi.com) event-driven APIs
with [Ash](https://hexdocs.pm/ash).

`AshJsonApi` and `AshGraphql` describe the requests a resource answers. AshAsyncApi
describes the messages it sends and receives — and then actually sends and receives
them. One declaration gives you three things:

1. **The AsyncAPI document**, generated from your resources, with JSON Schema payloads
   derived from your attributes and action inputs.
2. **Outbound publishing**, driven off Ash notifications, so opening a ticket puts a
   message on a topic without you writing any glue.
3. **Inbound dispatch**, so a message arriving on a topic runs an Ash action, with the
   payload narrowed to what that action actually accepts.

## Example

```elixir
defmodule Helpdesk.Support.Ticket do
  use Ash.Resource,
    domain: Helpdesk.Support,
    extensions: [AshAsyncApi.Resource]

  async_api do
    type "ticket"

    channels do
      # Addresses are segment lists. The delimiter comes from the bus, so this is
      # helpdesk/tickets/<id>/events on MQTT and helpdesk.tickets.<id>.events on NATS.
      channel :ticket_events, ["helpdesk", "tickets", :id, "events"] do
        description "Lifecycle events for a single ticket"
      end

      channel :ticket_commands, ["helpdesk", "tickets", "commands"]
    end

    operations do
      publish :open, :ticket_events do
        message_name "ticketOpened"
        payload_fields [:id, :subject, :status, :priority]
      end

      publish :close, :ticket_events
      subscribe :open, :ticket_commands
    end
  end

  # ... attributes and actions, as usual
end
```

```elixir
defmodule Helpdesk.Support do
  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    info do
      title "Helpdesk Events"
      version "1.0.0"
    end

    servers do
      server :mqtt, "broker.example.com:1883" do
        protocol :mqtt
        transport AshAsyncApi.Transport.Mqtt
      end
    end
  end

  resources do
    resource Helpdesk.Support.Ticket
  end
end
```

```elixir
defmodule Helpdesk.AsyncApiRouter do
  use AshAsyncApi.Router, domains: [Helpdesk.Support]
end
```

Add the router to your supervision tree and that is the whole setup. Opening a ticket
publishes to `helpdesk/tickets/<id>/events`; a message on `helpdesk/tickets/commands`
opens one.

```elixir
# describe
Helpdesk.AsyncApiRouter.spec_json()

# observe, from anywhere in the cluster
AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, :ticket_events)

receive do
  {:ash_async_api, envelope} -> envelope.payload
end

# publish by hand when you need to
AshAsyncApi.publish_to(Helpdesk.AsyncApiRouter, :ticket_events, %{status: "escalated"},
  params: %{id: ticket.id}
)
```

## Addresses

A channel address is a list of segments. You never write the delimiter, because it belongs
to the broker rather than to your API:

```elixir
channel :ticket_events, ["helpdesk", "tickets", :id, "events"]

# helpdesk/tickets/<id>/events   on MQTT   (and subscribes with helpdesk/tickets/+/events)
# helpdesk.tickets.<id>.events   on NATS   (and subscribes with helpdesk.tickets.*.events)
# helpdesk:tickets:<id>:events   on Redis
```

Segments interleave literals, fields and relationship paths, so an address can carry as much
of a record's identity as you want to route on:

```elixir
channel :comment_events,
        ["helpdesk", [:ticket, :organization_id], "tickets", [:ticket, :id], "comments", :id]

# helpdesk/acme/tickets/9f2.../comments/41b...
```

`[:ticket, :organization_id]` walks the relationship — reading the foreign key directly when
it is already on the record, and loading only when it is not. Paths are checked at compile
time, so a misspelled relationship fails the build rather than the publish.

## Every resource, every action: the CRUD event firehose

Services that publish a lifecycle event for *every* record change do not want to spell out
channels and operations per resource. Four **special segments** describe the declaration
site instead of a record field, and `publish_all` covers every action of a type — so one
`Spark.Dsl.Fragment` gives every resource its own fully described channel:

```elixir
defmodule MyApp.Events do
  use Spark.Dsl.Fragment, of: Ash.Resource, extensions: [AshAsyncApi.Resource]

  async_api do
    channels do
      channel :events, [:_domain, :_resource, :_event, :_pkey]
    end

    operations do
      publish_all :create, :events
      publish_all :update, :events
      publish_all :destroy, :events
    end
  end
end
```

Applied to `Crm.Lead`, that channel resolves to `crm.lead.{event}.{id}` (on NATS), and a
custom `create :import` action publishes to `crm.lead.created.9f2c...` — `publish_all`
matches actions by *type*, so custom-named actions are covered, and each expands into its
own operation in the generated document, payload schema included. An action that also has
an explicit `publish` on the same channel keeps it; `event_name "qualified"` overrides the
verb for actions whose past tense cannot be inferred. Composite primary keys join into one
`{pkey}` token, `a-b`.

## Runtime server configuration

The `server` declaration is the *description* — the document should show the real broker.
Which broker a node actually connects to is runtime configuration, passed through the
supervision tree:

```elixir
children = [
  {MyApp.AsyncApiRouter, servers: [nats: nats_config()]}
]

defp nats_config do
  case Application.get_env(:my_app, :nats) do
    nil -> :disabled
    config -> [transport_opts: [connection_settings: [Map.new(config)]]]
  end
end
```

A `:disabled` server starts no transport and its publishes are dropped silently — dev and
test run with no broker and no error noise — while `AshAsyncApi.PubSub` keeps delivering
in-cluster. A keyword list merges over the compile-time `transport_opts`.

## How the pieces fit

```
  Ash action
  (notification)
        │
        ▼
  AshAsyncApi.Publisher ──────┬───────────────────────┐
                              │                       │
                              ▼                       ▼
                  AshAsyncApi.PubSub            Transport
                       (Group)                (MQTT/NATS/Kafka)
                              │                       │
                              ▼                       ▼
                  subscribers on every           the broker
                  node in the cluster                 │
                                                      ▼
                                        AshAsyncApi.Router.Inbound
                                     match address → run action → fan out
```

The middle layer is the interesting one. A broker connection lives on exactly one node —
whichever happens to hold the MQTT socket — but the processes that want the message could
be anywhere in the cluster. So inbound messages are handed to
[`Group`](https://group.hexdocs.pm/Group.html), which distributes them over Erlang
distribution to every subscriber on every node.

That is what makes the transport swappable: everything above it is identical whether you
are running MQTT, NATS, Kafka, or no broker at all.

Note the deliberate asymmetry on the way in. `subscribe` operations run **once**, on the
node that received the message, because actions have effects. Subscribed processes are
observers, so **all** of them see it.

## Transports

| Transport | Library | Wildcards | Notes |
| --------- | ------- | --------- | ----- |
| `AshAsyncApi.Transport.Local` | none | `+` | In-cluster only. Starts no processes. |
| `AshAsyncApi.Transport.Mqtt` | [`emqtt`](https://hex.pm/packages/emqtt) | `+` | Segments join with `/`; levels map onto parameters exactly. |
| `AshAsyncApi.Transport.Nats` | [`gnat`](https://hex.pm/packages/gnat) | `*` | Segments join with `.`. Use a `:queue_group` so one node handles each message. |
| `AshAsyncApi.Transport.Kafka` | [`brod`](https://hex.pm/packages/brod) | none | Segments join with `.`; the literal prefix is the topic and parameters are the message key. |

The client libraries are **not** dependencies of AshAsyncApi — add the one you need. See
[Transports](documentation/topics/transports.md) for the details, including how to write
your own.

## Runnable example

[`examples/helpdesk`](examples/helpdesk/README.md) is a Docker Compose stack with **two clustered
nodes**, **Mosquitto** and **NATS**:

```sh
cd examples/helpdesk
docker compose up --build
./bin/demo.sh
```

`bin/demo.sh` checks ten behaviours and prints ✓/✗ for each — auto-publishing, cross-node
fan-out, inbound dispatch, NATS queue-group deduplication, `filter`, `hide_fields`,
per-entity subscriptions, spec generation, and one segment list coming out `/`-joined on
MQTT and `.`-joined on NATS.

Storage is per-node ETS on purpose: when node2 logs an event for a ticket that exists only in
node1's table, the message can only have arrived via `Group`.

## Installation

```elixir
def deps do
  [
    {:ash_async_api, "~> 0.1.0"},

    # plus a client library for your broker, if you are using one
    {:emqtt, "~> 1.13"}
  ]
end
```

Then read the [Getting Started
guide](documentation/tutorials/getting-started-with-ash-async-api.md).

## Generating the document

```sh
mix ash_async_api.spec --router Helpdesk.AsyncApiRouter --output asyncapi.json
mix ash_async_api.spec --check   # fails if the checked-in file is stale
```

The output is a valid AsyncAPI 3.0 document, so the whole
[AsyncAPI toolchain](https://www.asyncapi.com/tools) — docs, validators, client
generators — works on it.

## Documentation

- [Getting Started](documentation/tutorials/getting-started-with-ash-async-api.md)
- [What is AshAsyncApi?](documentation/topics/what-is-ash-async-api.md)
- [Transports](documentation/topics/transports.md)
- [`AshAsyncApi.Resource` DSL](documentation/dsls/DSL-AshAsyncApi.Resource.md)
- [`AshAsyncApi.Domain` DSL](documentation/dsls/DSL-AshAsyncApi.Domain.md)

## Status

The DSL, spec generation, `AshAsyncApi.PubSub`, and the `Local` transport are covered by
the test suite. The MQTT, NATS and Kafka transports are implemented against those
libraries' documented APIs, and their address translation and configuration validation
are tested — but the suite runs no brokers, so their connection handling has not been
exercised against live infrastructure. Treat those three as needing a smoke test in your
environment.

## License

MIT
