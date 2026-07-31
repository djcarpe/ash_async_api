defmodule AshAsyncApi do
  @moduledoc """
  Build [AsyncAPI 3.0](https://www.asyncapi.com) event-driven APIs with
  [Ash](https://hexdocs.pm/ash).

  Where `AshJsonApi` and `AshGraphql` describe the requests a resource answers,
  AshAsyncApi describes the messages it sends and receives — and then actually sends
  and receives them. Declaring an operation gives you three things at once: the
  AsyncAPI document, the outbound publishing, and the inbound action dispatch.

      defmodule Helpdesk.Support.Ticket do
        use Ash.Resource,
          domain: Helpdesk.Support,
          extensions: [AshAsyncApi.Resource]

        async_api do
          type "ticket"

          channels do
            # A segment list. The delimiter comes from the bus carrying the channel.
            channel :ticket_events, ["helpdesk", "tickets", :id, "events"]
          end

          operations do
            publish :open, :ticket_events
            publish :close, :ticket_events
          end
        end
      end

  Opening a ticket now publishes a `ticketOpened` message to
  `helpdesk/tickets/<id>/events` on MQTT — or `helpdesk.tickets.<id>.events` on NATS, from
  the same declaration — and `Helpdesk.AsyncApiRouter.spec()` describes it.

  ## The three layers

  | Layer | Module | Concern |
  | ----- | ------ | ------- |
  | Description | `AshAsyncApi.Spec` | Generating the AsyncAPI 3.0 document |
  | Distribution | `AshAsyncApi.PubSub` | Cluster-wide fan-out, provider-independent |
  | Transport | `AshAsyncApi.Transport` | Bytes on the wire for one broker |

  The middle layer is the interesting one. Transports are per-broker and connect on
  one node, but subscribers live all over the cluster, so inbound messages are handed
  to [`Group`](https://group.hexdocs.pm/Group.html) and distributed over Erlang
  distribution. Everything above the transport is identical whether you are running
  MQTT, NATS, Kafka, or nothing at all.

  ## Getting started

  See the [getting started guide](getting-started-with-ash-async-api.md).
  """

  alias AshAsyncApi.Publisher

  @doc """
  Publish a message.

  The three forms:

      # every `publish` operation bound to this action
      AshAsyncApi.publish(MyRouter, ticket, action: :open)

      # an envelope you built, e.g a reply
      AshAsyncApi.publish(MyRouter, envelope)

      # for the notifier
      AshAsyncApi.publish(MyRouter, notification)

  Every published message is broadcast to subscribers across the cluster *and* handed
  to the channel's transports. See `AshAsyncApi.Publisher` for why in that order.

  ## Options

    * `:action` — required when publishing a record. Which action's operations to
      publish.
    * `:broadcast?` — whether to fan out via `AshAsyncApi.PubSub`. Defaults to `true`.
    * `:transports?` — whether to hand the message to transports. Defaults to `true`.
      `false` keeps a message inside the cluster.
  """
  @spec publish(module(), term(), keyword()) ::
          {:ok, AshAsyncApi.Envelope.t() | [AshAsyncApi.Envelope.t()]} | {:error, term()}
  def publish(router, subject, opts \\ [])

  def publish(router, %Ash.Notifier.Notification{} = notification, _opts) do
    Publisher.publish_notification(router, notification)
  end

  def publish(router, %AshAsyncApi.Envelope{} = envelope, opts) do
    Publisher.publish_envelope(router, envelope, opts)
  end

  def publish(router, record, opts) when is_struct(record) do
    Publisher.publish_record(router, record, opts)
  end

  @doc """
  Publish a payload directly onto a channel, with no operation involved.

  For messages that are not the result of a resource action — heartbeats, commands to
  another service, ad-hoc events.

      AshAsyncApi.publish_to(MyRouter, :ticket_events, %{status: "escalated"},
        params: %{ticket_id: ticket.id}
      )

  ## Options

    * `:params` — values for the channel address's `{parameters}`.
    * `:message` — the message name, matched against `subscribe` operations by
      consumers.
    * `:headers`, `:content_type`, `:correlation_id`, `:reply_to` — set on the envelope.
  """
  @spec publish_to(module(), atom(), term(), keyword()) ::
          {:ok, AshAsyncApi.Envelope.t()} | {:error, term()}
  def publish_to(router, channel, payload, opts \\ []) do
    Publisher.publish_to(router, channel, payload, opts)
  end

  @doc """
  Subscribe the calling process to a channel or a concrete address.

  Messages arrive as `{:ash_async_api, %AshAsyncApi.Envelope{}}`.

      # every ticket
      AshAsyncApi.subscribe(MyRouter, :ticket_events)

      # one ticket
      AshAsyncApi.subscribe(MyRouter, "helpdesk/tickets/\#{id}/events")

  Subscriptions are cluster-wide and are dropped automatically when the subscribing
  process dies.

  `meta` is stored with the subscription and visible to `subscribers/2`, which lets you
  filter a fan-out without waking every process.

  Note that `Group` is eventually consistent, so a subscription may miss a message
  broadcast from another node in the milliseconds right after subscribing. Subscribe
  first, then read current state.
  """
  @spec subscribe(module(), atom() | String.t(), map()) :: :ok | {:error, term()}
  def subscribe(router, channel_or_address, meta \\ %{}) do
    AshAsyncApi.PubSub.subscribe(router, channel_or_address, meta)
  end

  @doc """
  Unsubscribe the calling process from a channel or address.
  """
  @spec unsubscribe(module(), atom() | String.t()) :: :ok | {:error, term()}
  def unsubscribe(router, channel_or_address) do
    AshAsyncApi.PubSub.unsubscribe(router, channel_or_address)
  end

  @doc """
  The subscribers to a channel or address, as `{pid, meta}` pairs.
  """
  @spec subscribers(module(), atom() | String.t()) :: [{pid(), map()}]
  def subscribers(router, channel_or_address) do
    AshAsyncApi.PubSub.subscribers(router, channel_or_address)
  end

  @doc """
  How many processes are subscribed, across the cluster.

  Worth checking before building an expensive payload.
  """
  @spec subscriber_count(module(), atom() | String.t()) :: non_neg_integer()
  def subscriber_count(router, channel_or_address) do
    AshAsyncApi.PubSub.subscriber_count(router, channel_or_address)
  end

  @doc """
  The AsyncAPI 3.0 document for a router, as a map. See `AshAsyncApi.Spec.generate/2`.
  """
  @spec spec(module(), keyword()) :: map()
  def spec(router, opts \\ []), do: AshAsyncApi.Spec.generate(router, opts)

  @doc """
  The envelope that caused the currently running action, if any.

  Available inside changes, validations and action implementations reached through a
  `subscribe` operation, so an action can see the address it was addressed at, the
  correlation id, or the raw headers:

      def change(changeset, _opts, context) do
        case AshAsyncApi.envelope(context) do
          nil -> changeset
          envelope -> Ash.Changeset.force_change_attribute(changeset, :source, envelope.address)
        end
      end
  """
  @spec envelope(map()) :: AshAsyncApi.Envelope.t() | nil
  def envelope(%{ash_async_api: %{envelope: envelope}}), do: envelope
  def envelope(%{context: context}) when is_map(context), do: envelope(context)
  def envelope(_), do: nil
end
