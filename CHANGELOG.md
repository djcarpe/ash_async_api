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
- `AshAsyncApi.Address` — address template compilation, interpolation, matching, and
  translation to each broker's wildcard syntax.
- `mix ash_async_api.spec` — write the document to a file, with `--check` for CI.
- Compile-time verifiers for channel parameters, action existence, field references,
  operation name uniqueness, server references, and per-transport configuration.
