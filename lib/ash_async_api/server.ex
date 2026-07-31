defmodule AshAsyncApi.Server do
  @moduledoc """
  A server is a broker the application talks to, plus the
  `AshAsyncApi.Transport` implementation that knows how to talk to it.

  This is the seam between the AsyncAPI *description* and the running system: the
  `host`/`protocol`/`pathname` fields render into the AsyncAPI document, while
  `transport` and `transport_opts` decide what actually moves bytes.

      servers do
        server :mqtt, "broker.example.com:1883" do
          protocol :mqtt
          protocol_version "5"
          transport AshAsyncApi.Transport.Mqtt
          transport_opts [client_id: "helpdesk"]
        end
      end

  Maps onto the AsyncAPI 3.0 [Server
  Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#serverObject).
  """

  defstruct [
    :name,
    :host,
    :protocol,
    :protocol_version,
    :pathname,
    :title,
    :summary,
    :description,
    :external_docs,
    :transport,
    :delimiter,
    :__spark_metadata__,
    :__identifier__,
    transport_opts: [],
    variables: %{},
    security: [],
    tags: [],
    bindings: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          host: String.t() | nil,
          protocol: atom() | String.t() | nil,
          protocol_version: String.t() | nil,
          pathname: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          external_docs: String.t() | nil,
          transport: module() | nil,
          delimiter: String.t() | nil,
          transport_opts: keyword(),
          variables: map(),
          security: list(),
          tags: [String.t()],
          bindings: map()
        }

  @known_protocols [
    :amqp,
    :amqp1,
    :anypointmq,
    :googlepubsub,
    :http,
    :https,
    :ibmmq,
    :jms,
    :kafka,
    :"kafka-secure",
    :mercure,
    :mqtt,
    :mqtts,
    :nats,
    :pulsar,
    :redis,
    :sns,
    :solace,
    :sqs,
    :stomp,
    :ws,
    :wss,
    # Not in the AsyncAPI protocol registry — used by AshAsyncApi.Transport.Local
    # to describe in-cluster delivery over Erlang distribution.
    :erlang
  ]

  @schema [
    name: [
      type: :atom,
      required: true,
      doc: "The name of the server, used to refer to it from channels."
    ],
    host: [
      type: :string,
      required: true,
      doc:
        "The server host name, optionally including the port. It must not include the protocol."
    ],
    protocol: [
      type: {:or, [{:in, @known_protocols}, :string]},
      required: true,
      doc: "The protocol this server supports for connection, e.g `:mqtt` or `:kafka`."
    ],
    protocol_version: [
      type: :string,
      doc: "The version of the protocol used for connection, e.g `\"5\"` for MQTT 5."
    ],
    pathname: [
      type: :string,
      doc: "The path to a resource in the host, e.g `/v2`."
    ],
    delimiter: [
      type: :string,
      doc: """
      What joins the segments of a channel address on this bus. Defaults to the convention
      for the server's `protocol` — `/` for MQTT, `.` for NATS, Kafka and AMQP, `:` for
      Redis. Set this only when your broker is configured against convention.
      """
    ],
    transport: [
      type: {:behaviour, AshAsyncApi.Transport},
      doc: """
      The `AshAsyncApi.Transport` implementation that connects to this server. When
      omitted, the server is description-only — it appears in the generated
      AsyncAPI document but nothing is started for it.
      """
    ],
    transport_opts: [
      type: :keyword_list,
      default: [],
      doc: "Options passed to the transport's `start_link/1`."
    ],
    title: [
      type: :string,
      doc: "A human friendly title for the server."
    ],
    summary: [
      type: :string,
      doc: "A short summary of the server."
    ],
    description: [
      type: :string,
      doc: "A longer description of the server. CommonMark is allowed."
    ],
    variables: [
      type: :map,
      default: %{},
      doc: """
      A map of server variables used for substitution in the host, e.g
      `%{"port" => %{default: "1883"}}`.
      """
    ],
    security: [
      type: {:list, :any},
      default: [],
      doc: "A list of security scheme names or maps that apply to this server."
    ],
    tags: [
      type: {:list, :string},
      default: [],
      doc: "Tags for logically grouping servers."
    ],
    external_docs: [
      type: :string,
      doc: "A URL pointing to external documentation for this server."
    ],
    bindings: [
      type: :map,
      default: %{},
      doc: "Protocol specific information, keyed by protocol name."
    ]
  ]

  @doc false
  def schema, do: @schema

  @doc false
  def known_protocols, do: @known_protocols

  @doc false
  def entity do
    %Spark.Dsl.Entity{
      name: :server,
      describe: "Declare a broker the application connects to.",
      examples: [
        """
        server :mqtt, "broker.example.com:1883" do
          protocol :mqtt
          transport AshAsyncApi.Transport.Mqtt
        end
        """
      ],
      args: [:name, :host],
      target: __MODULE__,
      schema: @schema,
      identifier: :name
    }
  end

  # Every bus has a convention for how address segments are joined, and it is a property of
  # the protocol rather than of your API — which is why `AshAsyncApi.Address` takes a segment
  # list and lets the server supply the delimiter.
  @protocol_delimiters %{
    # Hierarchical topic paths.
    mqtt: "/",
    mqtts: "/",
    ws: "/",
    wss: "/",
    http: "/",
    https: "/",
    mercure: "/",
    # Dotted subjects and routing keys.
    nats: ".",
    kafka: ".",
    "kafka-secure": ".",
    amqp: ".",
    amqp1: ".",
    stomp: ".",
    jms: ".",
    pulsar: ".",
    googlepubsub: ".",
    solace: ".",
    ibmmq: ".",
    anypointmq: ".",
    sns: ".",
    sqs: ".",
    # Redis convention.
    redis: ":",
    # In-cluster delivery has no broker convention; `/` reads best.
    erlang: "/"
  }

  @default_delimiter "/"

  @doc """
  The delimiter that joins address segments on this server.

  Resolution order: the server's explicit `delimiter`, then the transport's
  `c:AshAsyncApi.Transport.default_delimiter/0` if it defines one, then the convention for
  the protocol, then `"/"`.

  ## Examples

      iex> AshAsyncApi.Server.delimiter(%AshAsyncApi.Server{protocol: :nats})
      "."

      iex> AshAsyncApi.Server.delimiter(%AshAsyncApi.Server{protocol: :mqtt})
      "/"

      iex> AshAsyncApi.Server.delimiter(%AshAsyncApi.Server{protocol: :nats, delimiter: "/"})
      "/"
  """
  @spec delimiter(t()) :: String.t()
  def delimiter(%__MODULE__{delimiter: delimiter}) when is_binary(delimiter), do: delimiter

  def delimiter(%__MODULE__{transport: transport, protocol: protocol}) do
    transport_delimiter(transport) || default_delimiter_for(protocol)
  end

  defp transport_delimiter(nil), do: nil

  defp transport_delimiter(transport) do
    if Code.ensure_loaded?(transport) and function_exported?(transport, :default_delimiter, 0) do
      transport.default_delimiter()
    end
  end

  @doc """
  The conventional delimiter for a protocol.

  ## Examples

      iex> AshAsyncApi.Server.default_delimiter_for(:kafka)
      "."

      iex> AshAsyncApi.Server.default_delimiter_for(:redis)
      ":"

      iex> AshAsyncApi.Server.default_delimiter_for(:something_unknown)
      "/"
  """
  @spec default_delimiter_for(atom() | String.t() | nil) :: String.t()
  def default_delimiter_for(protocol) when is_binary(protocol) do
    default_delimiter_for(String.to_existing_atom(protocol))
  rescue
    ArgumentError -> @default_delimiter
  end

  def default_delimiter_for(protocol) do
    Map.get(@protocol_delimiters, protocol, @default_delimiter)
  end

  @doc false
  def protocol_delimiters, do: @protocol_delimiters

  @doc """
  The full URL for a server, as `protocol://host/pathname`.
  """
  def url(%__MODULE__{host: host, protocol: protocol, pathname: pathname}) do
    base = "#{protocol}://#{host}"

    case pathname do
      nil -> base
      "" -> base
      "/" <> _ = path -> base <> path
      path -> base <> "/" <> path
    end
  end
end
