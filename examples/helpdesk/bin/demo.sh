#!/usr/bin/env bash
#
# Drives the demo from outside the containers. Run `docker compose up --build` first, then:
#
#   ./bin/demo.sh
#
set -uo pipefail

cd "$(dirname "$0")/.."

DC="docker compose"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }
ok()   { printf '\033[32m  ✓ %s\033[0m\n' "$*"; }
bad()  { printf '\033[31m  ✗ %s\033[0m\n' "$*"; }

rpc() { $DC exec -T "$1" bin/rpc "$2"; }

# ──────────────────────────────────────────────────────────────────────────────────────
bold "0. Waiting for both nodes to form a cluster"

for _ in $(seq 1 60); do
  peers=$(rpc node1 'AshAsyncApi.PubSub.nodes(Helpdesk.AsyncApiRouter)' 2>/dev/null || true)
  if [[ "$peers" == *"helpdesk@node2"* ]]; then
    ok "node1 sees node2 in the Group cluster: $peers"
    break
  fi
  sleep 2
done

if [[ "${peers:-}" != *"helpdesk@node2"* ]]; then
  bad "nodes did not cluster. Try: docker compose logs node1 node2"
  exit 1
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "1. Start watching MQTT, so we can see what leaves the application"
dim   "   mosquitto_sub -h mosquitto -t 'helpdesk/#' -v"

MQTT_LOG=$(mktemp)
$DC exec -T mqtt-cli mosquitto_sub -h mosquitto -t 'helpdesk/#' -v >"$MQTT_LOG" 2>/dev/null &
MQTT_PID=$!
trap 'kill $MQTT_PID 2>/dev/null; rm -f "$MQTT_LOG"' EXIT
sleep 2

# ──────────────────────────────────────────────────────────────────────────────────────
bold "2. Open a ticket on node1 — no publishing code involved, just Ash.create!"
dim   "   Helpdesk.Demo.open(\"Printer on fire\")"

rpc node1 'Helpdesk.Demo.open("Printer on fire").id'
sleep 2

bold "3. What reached MQTT"
if grep -q ticketOpened "$MQTT_LOG"; then
  ok "a ticketOpened message was published"
  dim "$(grep -m1 ticketOpened "$MQTT_LOG" | cut -c1-200)"
else
  bad "nothing on MQTT yet — check: docker compose logs node1"
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "4. The point of the demo: node2 has no ticket, but did get the event"
dim   "   node2's ETS store is separate, and it has no MQTT subscription to this topic."
dim   "   The only path the message could take is Group, over Erlang distribution."

echo "  node2 tickets in its own store:"
rpc node2 'length(Helpdesk.Demo.tickets())' | sed 's/^/    /'

echo "  node2 audit log entries:"
rpc node2 'Helpdesk.Demo.audit() |> Enum.map(& &1.message)' | sed 's/^/    /'

if rpc node2 'Helpdesk.Demo.audit() |> Enum.map(& &1.message)' | grep -q ticketOpened; then
  ok "node2 received the event it has no data for — cluster fan-out works"
else
  bad "node2 did not receive the event"
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "5. Escalate it twice. The filter should let the first through and drop the second."
dim   "   filter fn ticket, notification -> ... notification.changeset.data.priority end"

rpc node1 'Helpdesk.Demo.escalate_latest().priority'
sleep 2
escalations_after_first=$(grep -c ticketEscalated "$MQTT_LOG" || true)

rpc node1 'Helpdesk.Demo.escalate_latest().priority'
sleep 2
escalations_after_second=$(grep -c ticketEscalated "$MQTT_LOG" || true)

echo "  ticketEscalated messages — after 1st escalate: $escalations_after_first, after 2nd: $escalations_after_second"

if [[ "$escalations_after_first" == "1" && "$escalations_after_second" == "1" ]]; then
  ok "the no-op escalation published nothing (filter works)"
elif [[ "$escalations_after_first" == "0" ]]; then
  bad "the real escalation did not publish"
else
  bad "expected exactly 1 ticketEscalated, got $escalations_after_second"
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "6. Inbound: send a command over NATS and let it run an Ash action"
dim   "   nats pub helpdesk.tickets.commands '{\"message\":\"openTicket\", ...}'"

before1=$(rpc node1 'length(Helpdesk.Demo.tickets())' | tr -d ' ')
before2=$(rpc node2 'length(Helpdesk.Demo.tickets())' | tr -d ' ')

$DC exec -T nats-cli nats --server nats://nats:4222 pub helpdesk.tickets.commands \
  '{"message":"openTicket","payload":{"subject":"Opened by another service","priority":"low"}}' \
  >/dev/null 2>&1

sleep 3

after1=$(rpc node1 'length(Helpdesk.Demo.tickets())' | tr -d ' ')
after2=$(rpc node2 'length(Helpdesk.Demo.tickets())' | tr -d ' ')

created=$(( (after1 - before1) + (after2 - before2) ))

echo "  tickets created — node1: $((after1 - before1)), node2: $((after2 - before2))"

if [[ "$created" == "1" ]]; then
  ok "exactly one node handled the command (NATS queue group did its job)"
elif [[ "$created" == "0" ]]; then
  bad "no ticket was created from the NATS command"
else
  bad "$created tickets created — the queue group is not deduplicating"
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "7. hide_fields cuts both ways"

if grep -q internal_notes "$MQTT_LOG"; then
  bad "internal_notes leaked onto the wire"
else
  ok "internal_notes is set on every ticket but never published"
fi

dim "   Now try to set it from outside — the action accepts it, but hide_fields does not."

$DC exec -T nats-cli nats --server nats://nats:4222 pub helpdesk.tickets.commands \
  '{"payload":{"subject":"Trying to set internal notes","internal_notes":"INJECTED"}}' \
  >/dev/null 2>&1

sleep 3

injected=$(rpc node1 'Enum.map(Helpdesk.Demo.tickets(), & &1.internal_notes)'; rpc node2 'Enum.map(Helpdesk.Demo.tickets(), & &1.internal_notes)')

if echo "$injected" | grep -q INJECTED; then
  bad "an inbound message set a hidden field"
else
  ok "the inbound message could not set internal_notes"
fi

# ──────────────────────────────────────────────────────────────────────────────────────
bold "8. The generated AsyncAPI 3.0 document"
dim   "   curl localhost:4000/asyncapi.json"

curl -fsS localhost:4000/asyncapi.json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("  asyncapi:  ", d["asyncapi"]); print("  servers:   ", ", ".join(sorted(d.get("servers",{})))); print("  channels:  ", ", ".join(sorted(d.get("channels",{})))); print("  operations:", ", ".join(sorted(d.get("operations",{}))))' \
  || bad "could not fetch the spec from localhost:4000"

# ──────────────────────────────────────────────────────────────────────────────────────
bold "9. Watch a single ticket from the other node"
dim   "   node2 subscribes to one ticket's address; node1 publishes to it."
dim   "   Note the Process.sleep: Group is eventually consistent, so a subscription needs"
dim   "   a moment to reach node1 before node1 can dispatch to it."

watched=$($DC exec -T node2 bin/rpc '
  [t | _] = :rpc.call(:helpdesk@node1, Helpdesk.Demo, :tickets, [])
  parent = self()

  spawn_link(fn ->
    AshAsyncApi.subscribe(Helpdesk.AsyncApiRouter, "helpdesk/tickets/#{t.id}/events")
    send(parent, :ready)

    receive do
      {:ash_async_api, e} -> send(parent, {:got, e.message, e.payload})
    after
      8000 -> send(parent, :timeout)
    end
  end)

  receive do :ready -> :ok end
  Process.sleep(500)

  :rpc.call(:helpdesk@node1, AshAsyncApi, :publish_to, [
    Helpdesk.AsyncApiRouter, :ticket_events, %{note: "hand-rolled"},
    [params: %{ticket_id: t.id}, message: "adHoc"]
  ])

  receive do msg -> msg after 9000 -> :no_message end
' 2>&1)

echo "  $watched"

if [[ "$watched" == *"hand-rolled"* ]]; then
  ok "an address-scoped subscriber on node2 received a message published on node1"
else
  bad "the per-ticket subscription did not receive the message"
fi

bold "Everything above ran against the working tree."
dim   "Poke at it yourself:"
dim   "  docker compose exec node1 bin/console"
dim   "  docker compose exec mqtt-cli mosquitto_sub -h mosquitto -t 'helpdesk/#' -v"
dim   "  curl -s localhost:4001/status | python3 -m json.tool"
