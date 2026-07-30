# What is AshAsyncApi?

AshAsyncApi is two things that usually live in different libraries:

1. A **description generator** — it turns your Ash resources into an
   [AsyncAPI 3.0](https://www.asyncapi.com) document.
2. A **messaging runtime** — it publishes those messages and dispatches incoming ones to
   Ash actions.

Keeping them together is the point. A hand-maintained AsyncAPI document drifts from the
code within weeks. When the document is generated from the same declarations that drive the
runtime, it cannot drift: the JSON Schema for `ticketOpened` is derived from the exact field
list that `AshAsyncApi.Payload` puts on the wire.

## The three layers

| Layer | Module | Concern |
| ----- | ------ | ------- |
| Description | `AshAsyncApi.Spec` | Generating the AsyncAPI document |
| Distribution | `AshAsyncApi.PubSub` | Cluster-wide fan-out, provider-independent |
| Transport | `AshAsyncApi.Transport` | Bytes on the wire, for one broker |

### Why the middle layer exists

A broker connection is a single point in the cluster. There is one MQTT socket, held by one
process, on one node. But the processes that care about a message — LiveViews, GenServers,
caches — are spread across every node.

Bridging that gap in each transport would mean writing the same distribution logic four
times, differently. So AshAsyncApi does it once, using
[`Group`](https://group.hexdocs.pm/Group.html), a distributed process registry built on
Erlang distribution:

```
MQTT broker ──▶ transport (node A) ──▶ AshAsyncApi.PubSub ──▶ subscribers (nodes A, B, C)
```

Swap MQTT for NATS and everything to the right of the transport is byte-identical. A
transport's entire job is: put bytes on the wire, and call
`AshAsyncApi.Transport.deliver/4` when bytes arrive.

This also means the whole system works with **no broker at all**.
`AshAsyncApi.Transport.Local` starts no processes, because `AshAsyncApi.PubSub` already
delivers across the cluster. For an application whose events never need to leave the
cluster, that is a complete and legitimate deployment — and for tests it means the full
publish path runs with no infrastructure.

### Two kinds of receiver, deliberately different

An inbound message is handled twice, in two different ways, and the difference matters:

- **`subscribe` operations run once**, on the node that received the message from the
  broker. Actions have effects. Running `Ticket.open` on all three nodes would create three
  tickets.
- **Subscribed processes all get it.** They are observers. A LiveView on node B that does
  not hear about a change shows stale data.

So `AshAsyncApi.Router.Inbound` runs the actions locally and fans the envelope out
cluster-wide. Getting this backwards is the classic distributed-pub/sub bug, so it is worth
being explicit about.

## What a declaration buys you

```elixir
publish :open, :ticket_events do
  message_name "ticketOpened"
  payload_fields [:id, :subject, :status]
end
```

From that one entry:

- The document gains an operation `ticket_open` with `action: send`, a message
  `ticketOpened`, and a JSON Schema payload with `id` as `format: uuid`, `subject` as a
  `string` with your `max_length`, and `status` as an `enum` of your `one_of` values.
- `AshAsyncApi.Notifier` publishes on every successful `Ticket.open`, after the transaction
  commits, with the address interpolated from the record.
- Anything subscribed to `:ticket_events` or to that ticket's address receives it.

## Design decisions worth knowing about

### Payloads are normalized, not raw

A `DateTime` in a payload becomes an ISO 8601 string, an atom becomes a string, a `Decimal`
becomes a string. This happens *before* the envelope reaches local subscribers, not just
before it hits the wire.

That costs a little convenience — you get `"open"`, not `:open` — and buys something more
valuable: a subscriber behaves identically whether the message came from the local publisher
or arrived from a broker. Without it, code that worked in dev would break the first time a
message made a round trip. It also means the payload actually matches the JSON Schema the
document published for it.

### Publishing broadcasts locally *and* to the broker

`AshAsyncApi.publish/3` always fans the envelope out through `AshAsyncApi.PubSub` and hands
it to the channel's transports. Local subscribers therefore see an event immediately,
without waiting for a broker round trip, and they keep working in tests where no broker
exists.

To stop the broker echo from delivering everything twice, every published message carries an
origin header naming the router and node that sent it, and inbound messages matching this
router *and* a node in this cluster are dropped. Both halves matter: the router check stops
a service consuming its own events, and the node check means a second deployment of the same
code — a blue/green cutover — still sees the other's messages.

The consequence: you cannot use a broker round trip to invoke your own action. That is not a
limitation worth working around. Within one application, call the action.

### Channels are identified by address

The DSL identifies a channel by name; AsyncAPI identifies it by address. Two resources may
reasonably use the same channel name for different addresses, so
`AshAsyncApi.Router.Table` assigns each distinct address one unique document key. When names
do not collide — nearly always — the key is just the name.

### The routing table is cached, not compiled

Operations can hold anonymous functions (`transform`, `filter`), which cannot be escaped
into a module attribute. So the table is built on first use and cached in
`:persistent_term`. Reads are cheap; if you redefine domains at runtime, call
`AshAsyncApi.Router.clear_table/1`.

## What AshAsyncApi does not do

- **It is not a broker.** `AshAsyncApi.Transport.Local` distributes within an Erlang
  cluster; it does not persist messages, replay them, or reach outside the cluster.
- **It does not guarantee exactly-once.** Kafka delivery is at-least-once by construction
  (offsets commit after processing), and MQTT depends on your QoS. Make `subscribe`
  operations idempotent — `upsert? true` on creates is usually the answer.
- **It does not do schema validation on the way in.** The payload is narrowed to what the
  action accepts and then handed to Ash, whose changesets and validations are the real
  gate. The generated JSON Schema describes the contract; Ash enforces it.

## Comparison to the sibling extensions

| | `AshJsonApi` | `AshGraphql` | `AshAsyncApi` |
| --- | --- | --- | --- |
| Describes | JSON:API + OpenAPI | GraphQL SDL | AsyncAPI 3.0 |
| Direction | request/response | request/response | send and receive |
| Transport | HTTP, via Plug | HTTP, via Absinthe | MQTT / NATS / Kafka / cluster |
| Entry point | a router you forward to | a schema | a router in your supervision tree |
| Runs actions from | an inbound request | an inbound query | an inbound message |

The shape is deliberately familiar: add an extension to the resource and the domain, declare
what you want exposed, and point a router at it.
