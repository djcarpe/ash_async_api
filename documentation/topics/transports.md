# Transports

A transport has two jobs: put bytes on the wire, and call
`AshAsyncApi.Transport.deliver/4` when bytes arrive. Everything else — matching addresses
to channels, running actions, fanning out across the cluster — is `AshAsyncApi`'s work and
is identical for every provider.

The client libraries are **not** dependencies of AshAsyncApi. Add the one you need, so you
are not compiling a Kafka client to talk to MQTT.

## Delimiters

A channel address is a list of segments. What joins them is decided by the bus, not by your
DSL, so one declaration works everywhere:

```elixir
channel :ticket_events, ["helpdesk", "tickets", :id, "events"]
```

| Protocol | Delimiter | Address becomes |
| -------- | --------- | --------------- |
| MQTT, WS, HTTP | `/` | `helpdesk/tickets/<id>/events` |
| NATS, Kafka, AMQP, JMS, Pulsar, SNS, SQS, Pub/Sub | `.` | `helpdesk.tickets.<id>.events` |
| Redis | `:` | `helpdesk:tickets:<id>:events` |
| anything else | `/` | |

Resolution order, most specific first:

1. the channel's own `delimiter`
2. the domain's `default_delimiter`
3. the transport's `c:AshAsyncApi.Transport.default_delimiter/0`, if it defines one
4. the convention for the server's `protocol`
5. `/`

You should rarely need 1 or 2. They exist for brokers configured against convention, and to
settle the one case AshAsyncApi cannot decide for you: a channel on two servers whose
conventions disagree. That raises `AshAsyncApi.Error.DelimiterConflict` when the router
builds its table, because a channel has exactly one address and guessing would be worse
than failing.

```elixir
# A channel deliberately spanning MQTT and NATS
channel :audit, ["helpdesk", "audit"] do
  servers [:mqtt, :nats]
  delimiter "."
end
```

## Wildcard translation

Brokers also disagree about how you subscribe to a *family* of addresses. A transport
declares its syntax via `c:AshAsyncApi.Transport.wildcard_style/0`, and
`AshAsyncApi.Address.to_filter/2` does the translating.

Given `["helpdesk", "tickets", :id, "events"]`:

| Transport | `wildcard_style/0` | Subscription filter |
| --------- | ------------------ | ------------------- |
| MQTT | `{:single, "+"}` | `helpdesk/tickets/+/events` |
| NATS | `{:single, "*"}` | `helpdesk.tickets.*.events` |
| Kafka | `:exact` | `helpdesk.tickets` (topic), parameters become the key |

Parameters are extracted back out of the concrete address on the way in, so `:id` is
available as action input and on the envelope, whatever the broker.

## MQTT

`AshAsyncApi.Transport.Mqtt`, built on [`emqtt`](https://hex.pm/packages/emqtt).

MQTT is the closest fit. Its topic model is `/`-separated levels with a `+` single-level
wildcard, which is exactly what an address segment and parameter mean.

```elixir
{:emqtt, "~> 1.13"}
```

```elixir
server :mqtt, "broker.example.com:1883" do
  protocol :mqtt
  protocol_version "5"
  transport AshAsyncApi.Transport.Mqtt
  transport_opts [
    clientid: "helpdesk",
    username: "helpdesk",
    password: {:system, "MQTT_PASSWORD"},
    clean_start: false,
    qos: 1
  ]
end
```

Notable options: `:qos` (default `1`), `:retain`, `:subscribe_qos`,
`:reconnect_interval`. Everything else is passed through to `:emqtt.start_link/1`. A
`{:system, "VAR"}` tuple is read from the environment at startup, so credentials stay out
of compiled code.

Per-message overrides go in channel or operation `bindings`, following the
[MQTT binding](https://github.com/asyncapi/bindings/tree/master/mqtt) spec — and they are
rendered into the generated document too:

```elixir
channel :ticket_snapshots, ["helpdesk", "tickets", :id] do
  bindings %{mqtt: %{qos: 1, retain: true}}
end
```

`retain: true` on a snapshot channel is a genuinely useful pattern: a new subscriber
immediately receives the last known state of every ticket.

### Reconnection

An unreachable broker is an expected condition, not a bug, so the connection process
reconnects rather than crashing — letting the supervisor restart-loop on a down broker
would take the router with it. Publishing while disconnected returns
`{:error, :disconnected}`. Subscriptions are re-established after every reconnect.

## NATS

`AshAsyncApi.Transport.Nats`, built on [`gnat`](https://hex.pm/packages/gnat).

NATS subjects are `.`-separated with a `*` single-token wildcard, and a server with
`protocol :nats` gets `.` automatically — so the channel is written exactly as it would be
for MQTT:

```elixir
channel :ticket_events, ["helpdesk", "tickets", :id, "events"]
# → helpdesk.tickets.<id>.events
```

```elixir
{:gnat, "~> 1.8"}
```

```elixir
server :nats, "nats.example.com:4222" do
  protocol :nats
  transport AshAsyncApi.Transport.Nats
  transport_opts [queue_group: "helpdesk"]
end
```

### Set a queue group

Without `:queue_group`, every node receives every message, and every node's `subscribe`
operations run — three nodes means three tickets created from one command. With a queue
group, NATS delivers each message to exactly one member, and `AshAsyncApi.PubSub` fans it
out from there.

This is the single most important NATS configuration decision, and it is easy to miss
because it works fine on one node.

## Kafka

`AshAsyncApi.Transport.Kafka`, built on [`brod`](https://hex.pm/packages/brod).

Kafka is the odd one out. It has no wildcards and no topic hierarchy, so a parameterised
address cannot be a topic. Instead the literal prefix becomes the topic and the parameters
become the **message key**:

```elixir
channel :ticket_events, ["helpdesk", "tickets", :id]
#  topic: "helpdesk.tickets"
#  key:   the ticket id
```

This is not a workaround. Kafka guarantees ordering within a partition, and keying by
entity id puts every event for one ticket on one partition, in order — which is what you
want anyway. On the way in the key is recombined with the topic to reconstruct the full
address, so parameter extraction works exactly as it does elsewhere.

```elixir
{:brod, "~> 4.0"}
```

```elixir
server :kafka, "kafka.example.com:9092" do
  protocol :kafka
  transport AshAsyncApi.Transport.Kafka
  transport_opts [
    client_id: :helpdesk_kafka,
    group_id: "helpdesk",
    endpoints: [{"kafka.example.com", 9092}]
  ]
end
```

`:group_id` is required, and AshAsyncApi refuses to compile without it — consumer groups
are what stop every node processing every message.

### Delivery scope and at-least-once

This transport reports `delivery_scope/0` as `:local`. Every node in a consumer group owns
its own partitions, so a message arriving on node A is already the only copy in the cluster;
re-broadcasting it to B and C would duplicate it, since they receive their own partitions
independently. Subscribers therefore see the messages belonging to their node's partitions.

Offsets commit after `AshAsyncApi.Transport.deliver/4` returns, giving at-least-once
processing. A crash mid-message means redelivery, so make `subscribe` operations on a Kafka
channel idempotent:

```elixir
subscribe :open, :ticket_commands do
  upsert? true
end
```

## Local

`AshAsyncApi.Transport.Local` starts no processes and opens no sockets, because
`AshAsyncApi.PubSub` already delivers across the cluster.

```elixir
server :cluster, "erlang-distribution" do
  protocol :erlang
  transport AshAsyncApi.Transport.Local
end
```

Use it for:

- **Tests.** The full publish path runs with no infrastructure.
- **Development.** Same.
- **Production**, when your events never need to leave the cluster. This is a real option,
  not a stub.

Its `publish/4` loops the message back through `AshAsyncApi.Transport.deliver/4` so
`subscribe` operations can run — subject to the router's `ignore_own_messages?`, which
defaults to `true`. To exercise the receive path end to end in a test, start the router with
`ignore_own_messages?: false`.

`AshAsyncApi.Spec` omits `Local` servers from the generated document by default, since
in-cluster delivery is not something an external consumer can connect to. Pass
`include_local?: true` to see it.

## Writing your own

Implement three callbacks. `use AshAsyncApi.Transport` supplies JSON `encode/2` and
`decode/2`, a no-op `unsubscribe/2`, and the MQTT-style defaults.

```elixir
defmodule MyApp.Transport.Redis do
  use AshAsyncApi.Transport

  alias AshAsyncApi.Transport.Context

  @impl true
  def wildcard_style, do: {:single, "*"}

  # Only needed when the protocol registry does not already know; `protocol :redis`
  # would give you ":" for free.
  @impl true
  def default_delimiter, do: ":"

  @impl true
  def child_spec(context) do
    %{
      id: Context.process_name(context),
      start: {MyApp.RedisConnection, :start_link, [context]}
    }
  end

  @impl true
  def publish(context, address, body, _opts) do
    MyApp.RedisConnection.publish(context, address, body)
  end

  @impl true
  def subscribe(context, filter) do
    MyApp.RedisConnection.psubscribe(context, filter)
  end
end
```

Then, whenever your connection process receives a message:

```elixir
AshAsyncApi.Transport.deliver(context, address, body, headers: headers)
```

That is the entire inbound contract. Address matching, action dispatch and cluster fan-out
all happen behind it.

### The context

`AshAsyncApi.Transport.Context` is handed to every callback and carries the router, the
domain, the `AshAsyncApi.Server` struct, the `Group` name, and the merged
`transport_opts`. Because it holds the router, your connection process does not need to
remember any of it — pass the context around and call `deliver/4`.

`Context.opt!/2` raises a message that says exactly where to set a missing option, which is
worth using for anything required.

### Optional callbacks

- `validate_opts/2` — reject bad configuration at **compile time**. This is where "you must
  supply a `:group_id`" belongs; the transport knows its own requirements, and finding out
  at compile time beats finding out when the supervisor fails to start.
- `delivery_scope/0` — return `:local` when every node consumes from the broker
  independently, so `AshAsyncApi.PubSub` does not multiply the message.
- `default_delimiter/0` — only when your bus joins address segments differently from what
  its protocol implies. See [Delimiters](#delimiters).
- `encode/2` / `decode/2` — override for a non-JSON wire format (Avro, Protobuf, MessagePack).

### Supervising transports yourself

`AshAsyncApi.Supervisor.transport_children/2` returns the child specs, so you can put them
under your own supervisor, start a subset, or start them at a different point in your tree.
Start the router with `start_transports?: false` if you want to own all of them.
