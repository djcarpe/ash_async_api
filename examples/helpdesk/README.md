# AshAsyncApi example: a two-node helpdesk

A runnable demonstration of `ash_async_api`: **two clustered Elixir nodes**, an **MQTT
broker**, and a **NATS server**.

```sh
cd examples/helpdesk
docker compose up --build     # first build takes a few minutes (emqtt compiles from source)

# in another terminal
./bin/demo.sh
```

The build context is the repository root and `ash_async_api` is a path dependency, so the
demo always runs against the working tree rather than a published version.

## What it demonstrates

| # | Claim | How you can see it |
| - | ----- | ------------------ |
| 1 | Publishing needs no publishing code | `Ash.create!` on node1 puts a message on MQTT |
| 2 | **Fan-out is cluster-wide and provider-independent** | node2's audit log prints an event for a ticket that exists only in node1's store |
| 3 | Addresses are structured, so consumers can watch one entity or one tenant | `Helpdesk.Demo.watch(ticket_id)` |
| 4 | An inbound message runs an Ash action | a NATS publish creates a ticket |
| 5 | A queue group gives exactly-once handling across nodes | only one of the two nodes creates the ticket |
| 6 | `filter` suppresses messages that should not be sent | escalating twice publishes once |
| 7 | `hide_fields` blocks both directions | `internal_notes` is never published, and a message cannot set it |
| 8 | The document cannot drift from the code | `curl localhost:4000/asyncapi.json` |
| 9 | A subscriber can watch one entity, from any node | node2 subscribes to one ticket, node1 publishes to it |
| 10 | **One segment list, two delimiters** | the MQTT channel joins with `/`, the NATS one with `.` — neither is written in the DSL |

`./bin/demo.sh` checks all ten and prints ✓/✗ for each, so it doubles as an integration
test. Every claim above was verified on a clean `docker compose build && up`.

### The one that matters: claim 2

Storage is `Ash.DataLayer.Ets`, deliberately **per node**. That is not laziness — it is what
makes the demo conclusive.

When node2's audit log prints `ticketOpened` for a ticket that lives only in node1's ETS
table, and node2 holds no MQTT subscription to that topic, there is exactly one path the
message could have taken: `AshAsyncApi.PubSub`, which is
[`Group`](https://group.hexdocs.pm/Group.html), over Erlang distribution.

A shared Postgres would have made this ambiguous — you could never tell whether the
subscriber received a message or just read the row.

## The architecture

```
                    ┌──────────── node1 ────────────┐   ┌──────────── node2 ────────────┐
  Ash.create!  ────▶ │ Notifier → Publisher          │   │                               │
                    │      │                        │   │                               │
                    │      ├──▶ PubSub (Group) ─────┼───┼──▶ AuditLog  ← the proof       │
                    │      │                        │   │                               │
                    │      └──▶ MQTT transport ─────┼───┼── MQTT transport               │
                    └───────────────┬───────────────┘   └───────────────┬───────────────┘
                                    │                                   │
                                    ▼                                   ▼
                             ┌─────────────┐                     ┌─────────────┐
                             │  mosquitto  │                     │    nats     │
                             │  (events)   │                     │ (commands)  │
                             └─────────────┘                     └─────────────┘
```

Two brokers, split by direction, to make the point that nothing above
`AshAsyncApi.Transport` knows or cares which broker a channel uses:

- **MQTT** carries outbound ticket events, one topic per ticket.
- **NATS** carries inbound commands under the queue group `helpdesk`, so exactly one node
  handles each command.
- **`Group`** carries in-cluster fan-out to `Helpdesk.AuditLog` on every node.

The same `AshAsyncApi.subscribe/2` call in `Helpdesk.AuditLog` sees both — events that
arrived over MQTT and commands that arrived over NATS.

Look at the two channel declarations in `lib/helpdesk/support/ticket.ex`: they are written
identically, as segment lists, with no delimiter anywhere.

```elixir
channel :ticket_events,   ["helpdesk", :organization_id, "tickets", :id, "events"]  # servers [:mqtt]
channel :ticket_commands, ["helpdesk", "tickets", "commands"]                        # servers [:nats]
```

At runtime they become `helpdesk/acme/tickets/<id>/events` and `helpdesk.tickets.commands`,
and they subscribe with `+` and `*` respectively — all of it decided by each server's
`protocol`. Step 10 of the demo prints both. Move a channel between the two servers and the
address, the filter and the generated document all follow.

## Poking at it by hand

```sh
# Watch everything leaving the app
docker compose exec mqtt-cli mosquitto_sub -h mosquitto -t 'helpdesk/#' -v

# Watch one organization only — organization_id is a segment, so it is routable
docker compose exec mqtt-cli mosquitto_sub -h mosquitto -t 'helpdesk/acme/#' -v

# Open a ticket on node1
docker compose exec node1 bin/rpc 'Helpdesk.Demo.open("Printer on fire")'

# See that node2 got the event but has no ticket
docker compose exec node2 bin/rpc 'Helpdesk.Demo.summary()'

# Send a command in from outside
docker compose exec nats-cli nats --server nats://nats:4222 pub helpdesk.tickets.commands \
  '{"message":"openTicket","payload":{"subject":"From another service"}}'

# A bare payload works too — for producers that know nothing about AshAsyncApi
docker compose exec nats-cli nats --server nats://nats:4222 pub helpdesk.tickets.commands \
  '{"subject":"No envelope at all"}'

# The generated document, and a status page
curl -s localhost:4000/asyncapi.json | python3 -m json.tool
curl -s localhost:4000/status | python3 -m json.tool
curl -s localhost:4001/status | python3 -m json.tool   # node2

# A real shell on a running node
docker compose exec node1 bin/console
```

In the console:

```elixir
# Subscribe to one ticket and watch it change
[ticket | _] = Helpdesk.Demo.tickets()
Helpdesk.Demo.watch(ticket.id)   # blocks until something happens to that ticket

# Who is listening?
AshAsyncApi.subscribers(Helpdesk.AsyncApiRouter, :ticket_events)
AshAsyncApi.PubSub.nodes(Helpdesk.AsyncApiRouter)

# Publish something that is not tied to an action
AshAsyncApi.publish_to(Helpdesk.AsyncApiRouter, :ticket_events, %{note: "hand-rolled"},
  params: %{organization_id: ticket.organization_id, id: ticket.id}
)
```

## Things worth noticing in the code

**`lib/helpdesk/support/ticket.ex`** — the whole messaging surface is about 40 lines of DSL.
There is no publishing code anywhere in this project; `AshAsyncApi.Notifier` does it off Ash
notifications, after the transaction commits.

**`lib/helpdesk/audit_log.ex`** — an ordinary GenServer. It subscribes in `init/1` *before*
reading any state, because `Group` is eventually consistent and reading first leaves a window
where an event can be missed.

That window is real and measurable, and step 9 of the demo shows it: a process that
subscribes on node2 and has node1 publish to it *immediately* receives nothing, because the
subscription has not replicated to node1 yet. A few hundred milliseconds is enough. This only
bites code that subscribes and instantly expects a message from another node — a long-lived
subscriber like `AuditLog` never notices — but it is why the rule is *subscribe, then read
current state*, never the reverse.

**`lib/helpdesk/cluster.ex`** — 40 lines of `Node.connect`. AshAsyncApi has no clustering
configuration of its own: `Group` finds peers through `:net_kernel.monitor_nodes/1`, so any
ordinary Erlang distribution setup works. `libcluster` would do just as well.

**`lib/helpdesk/support.ex`** — the two servers. Swapping which broker carries which channel
is a one-line change and touches nothing else.

## Two defaults that will surprise you

**Payloads are JSON-normalized before local delivery**, so `status` arrives as `"open"`, not
`:open`, even for a subscriber in the same VM as the publisher. This is deliberate: the same
envelope goes to local subscribers directly *and* to remote ones through a broker, and a
subscriber that behaved differently depending on which path a message took would be a trap.
It also means the payload matches the JSON Schema in the generated document.

**A router ignores messages it published itself** (`ignore_own_messages?`, default `true`), so
a channel used for both publishing and subscribing cannot loop. That is why this example has
separate event and command channels. Within one application you do not need a broker round
trip to call your own action — call the action.

## Known rough edge

Only one thing in this demo required care to avoid: **MQTT has no queue-group equivalent
exposed by AshAsyncApi**. If both nodes subscribed to an MQTT command topic, both would run
the action and you would get two tickets. MQTT 5 shared subscriptions (`$share/group/topic`)
are the answer, and AshAsyncApi has no way to request one yet.

That is why commands come in over NATS here, where `:queue_group` is supported. For an
MQTT-only deployment today, have a single designated consumer node start its transport with
`auto_subscribe?: false` on the others.

## Cleaning up

```sh
docker compose down -v
```
