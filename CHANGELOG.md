# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

Initial release.

### Added

- `AshAsyncApi.Resource` — resource extension with an `async_api` DSL for declaring
  `channels` (with templated addresses and parameters) and `operations`
  (`publish`/`subscribe`).
- `AshAsyncApi.Domain` — domain extension for document `info`, `servers`, and
  channels/operations declared on behalf of resources.
- `AshAsyncApi.Router` — `use AshAsyncApi.Router, domains: [...]` supervises the
  transports and the `Group` instance, and generates the document.
- `AshAsyncApi.Spec` — AsyncAPI 3.0.0 document generation, with JSON Schema payloads
  derived from Ash attributes, constraints and action inputs.
- `AshAsyncApi.PubSub` — cluster-wide, provider-independent fan-out built on
  [`Group`](https://group.hexdocs.pm/Group.html). Subscriptions by channel or by concrete
  address.
- `AshAsyncApi.Notifier` — automatic publishing off Ash notifications, after the
  transaction commits.
- `AshAsyncApi.Router.Inbound` — inbound dispatch: address matching, parameter
  extraction, payload narrowing, action execution, and request/reply.
- `AshAsyncApi.Transport` behaviour, with four implementations:
  - `AshAsyncApi.Transport.Local` — Erlang cluster only, no broker, no processes.
  - `AshAsyncApi.Transport.Mqtt` — via `emqtt`.
  - `AshAsyncApi.Transport.Nats` — via `gnat`.
  - `AshAsyncApi.Transport.Kafka` — via `brod`, mapping address parameters to message
    keys.
- `AshAsyncApi.Address` — addresses as **segment lists** (`["helpdesk", "tickets", :id]`)
  joined by the delimiter of whichever bus carries the channel, with interpolation, matching
  and translation to each broker's wildcard syntax. Segments interleave literals, fields and
  relationship paths (`[:organization, :id]`), which are checked at compile time and resolved
  without a query where the foreign key is already on the record. Plain string addresses with
  `{braces}` remain supported.
- Per-protocol delimiter defaults (`/` for MQTT, `.` for NATS/Kafka/AMQP, `:` for Redis),
  overridable per server, per domain and per channel, with an optional transport callback.
- `mix ash_async_api.spec` — write the document to a file, with `--check` for CI.
- Compile-time verifiers for channel parameters, action existence, field references,
  operation name uniqueness, server references, and per-transport configuration.
- `publish_all` — publish every action of a type (`:create`, `:update`, `:destroy`),
  including custom-named ones, expanded into one concrete operation per action when the
  routing table is built. An action that also has its own `publish` on the same channel
  keeps it. `event_name` on `publish`/`publish_all` overrides the event verb.
- Special address segments — `:_domain`, `:_resource`, `:_event`, `:_pkey` — resolved
  against the declaring scope at table build, so one fragment-declared channel like
  `[:_domain, :_resource, :_event, :_pkey]` becomes a distinct, concretely addressed
  channel per resource (`crm.lead.{event}.{id}`), each with its own payload schemas in
  the generated document. Composite primary keys join into a single `{pkey}` token.
  The domain's segment name is configurable with the new domain-level `type` option,
  and the `segment_naming` option controls how both the `:_domain` and
  `:_resource` segments render — `:snake` (default), `:camel`, or a one-argument
  function of the module for full control. Set it on the domain for a domain-wide
  policy, or on a resource to override for that resource's channels.
- Runtime server configuration — `{MyRouter, servers: [nats: [transport_opts: ...]]}`
  merges options over the compile-time declaration at startup, and
  `servers: [nats: :disabled]` skips that transport entirely: its publishes are dropped
  silently while in-cluster delivery over `AshAsyncApi.PubSub` keeps working. The same
  applies to every server under `start_transports?: false`.
- Address parameter values are sanitized on interpolation: the delimiter, whitespace and
  broker wildcard characters in a value are flattened to `_`, so data cannot leak into
  address structure.
