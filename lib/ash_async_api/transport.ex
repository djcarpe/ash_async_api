defmodule AshAsyncApi.Transport do
  @moduledoc """
  The behaviour a message broker adapter implements.

  A transport has exactly two jobs: put bytes on the wire, and hand bytes coming off
  the wire to `deliver/4`. Everything else — matching addresses to channels, running
  actions, fanning out to subscribers across the cluster — is `AshAsyncApi`'s and
  `AshAsyncApi.PubSub`'s work, and is identical for every provider.

  ## Writing one

      defmodule MyApp.Transport.Redis do
        use AshAsyncApi.Transport

        @impl true
        def wildcard_style, do: {:single, "*"}

        @impl true
        def child_spec(context) do
          {MyApp.RedisConnection, context: context, opts: context.opts}
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

  Then, whenever a message arrives, the connection process calls:

      AshAsyncApi.Transport.deliver(context, address, body, headers: headers)

  `use AshAsyncApi.Transport` supplies `encode/2`, `decode/2`, `unsubscribe/2` and
  `wildcard_style/0` defaults, so a minimal transport only needs `child_spec/1`,
  `publish/4` and `subscribe/2`.

  ## Wildcard styles

  Brokers disagree about how you subscribe to a family of addresses. A transport
  declares its syntax via `c:wildcard_style/0` and `AshAsyncApi.Address.to_filter/2`
  translates the channel template accordingly:

  | Broker | `wildcard_style/0`  | `tickets/{id}/events` becomes |
  | ------ | ------------------- | ----------------------------- |
  | MQTT   | `{:single, "+"}`    | `tickets/+/events`            |
  | NATS   | `{:single, "*"}`    | `tickets/*/events`            |
  | AMQP   | `{:single, "*"}`    | `tickets/*/events`            |
  | Kafka  | `:exact`            | `tickets`                     |
  """

  alias AshAsyncApi.Transport.Context

  @typedoc "Options accepted by `c:publish/4`, forwarded from channel and operation bindings."
  @type publish_opts :: keyword()

  @doc """
  The child spec for whatever process maintains the connection to this server.

  Return `nil` for a transport that needs no process of its own — see
  `AshAsyncApi.Transport.Local`, which rides on `AshAsyncApi.PubSub`.
  """
  @callback child_spec(Context.t()) ::
              Supervisor.child_spec() | {module(), term()} | module() | nil

  @doc """
  Put an encoded message on the wire at `address`.
  """
  @callback publish(Context.t(), address :: String.t(), body :: iodata(), publish_opts()) ::
              :ok | {:error, term()}

  @doc """
  Start receiving messages matching `filter`.

  `filter` is already in this transport's own wildcard syntax, per
  `c:wildcard_style/0`.
  """
  @callback subscribe(Context.t(), filter :: String.t()) :: :ok | {:error, term()}

  @doc """
  Stop receiving messages matching `filter`.
  """
  @callback unsubscribe(Context.t(), filter :: String.t()) :: :ok | {:error, term()}

  @doc """
  The wildcard syntax this broker subscribes with.
  """
  @callback wildcard_style() :: {:single, String.t()} | :multi_level | :exact

  @doc """
  Serialize an envelope for the wire. Defaults to JSON.
  """
  @callback encode(Context.t(), AshAsyncApi.Envelope.t()) :: {:ok, iodata()} | {:error, term()}

  @doc """
  Deserialize a wire body. Defaults to JSON, falling back to the raw binary.
  """
  @callback decode(Context.t(), body :: binary()) :: {:ok, term()} | {:error, term()}

  @doc """
  Reject bad configuration at compile time.

  Called by `AshAsyncApi.Domain.Verifiers.VerifyTransports` for every server using
  this transport.
  """
  @callback validate_opts(AshAsyncApi.Server.t(), keyword()) :: :ok | {:error, term()}

  @doc """
  Whether inbound messages should fan out cluster-wide or stay local.

  Return `:local` when every node consumes from the broker independently (a Kafka
  consumer group with a member per node), so the message is not delivered N times.
  Defaults to `:cluster`.
  """
  @callback delivery_scope() :: :cluster | :local

  @doc """
  The delimiter this bus joins address segments with.

  Only define this when the transport knows better than the protocol registry — the default
  comes from the server's `protocol`, via `AshAsyncApi.Server.default_delimiter_for/1`, which
  already covers MQTT (`/`), NATS/Kafka/AMQP (`.`) and Redis (`:`).
  """
  @callback default_delimiter() :: String.t()

  @optional_callbacks [
    unsubscribe: 2,
    encode: 2,
    decode: 2,
    validate_opts: 2,
    delivery_scope: 0,
    wildcard_style: 0,
    default_delimiter: 0
  ]

  @doc """
  Hand a message received from the broker to AshAsyncApi.

  This is the one function a transport calls on the inbound path. It decodes the
  body, works out which channel and operations the address belongs to, runs any
  `subscribe` operations, and fans the envelope out to subscribers across the
  cluster.

  ## Options

    * `:headers` — headers the broker delivered alongside the body.
    * `:content_type` — overrides the channel's content type.
    * `:correlation_id` / `:reply_to` — request/reply metadata from the broker's own
      header conventions.
    * `:metadata` — anything else worth carrying, e.g the Kafka partition and offset.
  """
  @spec deliver(Context.t(), String.t(), binary(), keyword()) ::
          {:ok, [AshAsyncApi.Envelope.t()]} | {:error, term()}
  def deliver(%Context{} = context, address, body, opts \\ []) do
    AshAsyncApi.Router.Inbound.deliver(context, address, body, opts)
  end

  @doc """
  The wildcard style for a transport, honouring the `{:single, "+"}` default.
  """
  @spec wildcard_style(module()) :: {:single, String.t()} | :multi_level | :exact
  def wildcard_style(transport) do
    if implements?(transport, :wildcard_style, 0) do
      transport.wildcard_style()
    else
      {:single, "+"}
    end
  end

  @doc """
  The delivery scope for a transport, defaulting to `:cluster`.
  """
  @spec delivery_scope(module()) :: :cluster | :local
  def delivery_scope(transport) do
    if implements?(transport, :delivery_scope, 0) do
      transport.delivery_scope()
    else
      :cluster
    end
  end

  # `function_exported?/3` answers "false" for a module that simply has not been loaded
  # yet, which would silently fall back to the MQTT defaults and make a NATS transport
  # subscribe with `+` instead of `*`. Loading first is the difference between an
  # optional callback and a wrong answer.
  defp implements?(module, function, arity) do
    Code.ensure_loaded?(module) and function_exported?(module, function, arity)
  end

  @doc """
  The default JSON encoding, exposed so transports can delegate to it.
  """
  @spec encode_json(AshAsyncApi.Envelope.t()) :: {:ok, iodata()} | {:error, term()}
  def encode_json(%AshAsyncApi.Envelope{} = envelope) do
    case Jason.encode_to_iodata(AshAsyncApi.Envelope.to_wire(envelope)) do
      {:ok, iodata} -> {:ok, iodata}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  The default JSON decoding.

  A body that is not valid JSON comes back as-is rather than as an error, so a
  transport carrying opaque binaries still works.
  """
  @spec decode_json(binary()) :: {:ok, term()}
  def decode_json(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _} -> {:ok, body}
    end
  end

  defmacro __using__(_opts) do
    quote do
      @behaviour AshAsyncApi.Transport

      @impl AshAsyncApi.Transport
      def wildcard_style, do: {:single, "+"}

      @impl AshAsyncApi.Transport
      def delivery_scope, do: :cluster

      @impl AshAsyncApi.Transport
      def unsubscribe(_context, _filter), do: :ok

      @impl AshAsyncApi.Transport
      def encode(_context, envelope), do: AshAsyncApi.Transport.encode_json(envelope)

      @impl AshAsyncApi.Transport
      def decode(_context, body), do: AshAsyncApi.Transport.decode_json(body)

      defoverridable wildcard_style: 0,
                     delivery_scope: 0,
                     unsubscribe: 2,
                     encode: 2,
                     decode: 2
    end
  end
end
