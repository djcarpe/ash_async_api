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
      channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
        description "Lifecycle events for a single ticket"
        parameter :ticket_id, source: :id
      end

      channel :ticket_commands, "helpdesk/tickets/commands"
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
  params: %{ticket_id: ticket.id}
)
```

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
| `AshAsyncApi.Transport.Mqtt` | [`emqtt`](https://hex.pm/packages/emqtt) | `+` | Topic levels map onto address parameters exactly. |
| `AshAsyncApi.Transport.Nats` | [`gnat`](https://hex.pm/packages/gnat) | `*` | Use a `:queue_group` so one node handles each message. |
| `AshAsyncApi.Transport.Kafka` | [`brod`](https://hex.pm/packages/brod) | none | Address prefix becomes the topic, parameters become the message key. |

The client libraries are **not** dependencies of AshAsyncApi — add the one you need. See
[Transports](documentation/topics/transports.md) for the details, including how to write
your own.

## Runnable example

[`examples/helpdesk`](examples/helpdesk) is a Docker Compose stack with **two clustered
nodes**, **Mosquitto** and **NATS**:

```sh
cd examples/helpdesk
docker compose up --build
./bin/demo.sh
```

`bin/demo.sh` checks nine behaviours and prints ✓/✗ for each — auto-publishing, cross-node
fan-out, inbound dispatch, NATS queue-group deduplication, `filter`, `hide_fields`,
per-entity subscriptions, and spec generation.

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
