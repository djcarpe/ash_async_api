defmodule AshAsyncApi.Channel do
  @moduledoc """
  A channel is an addressable conduit for messages — an MQTT topic, a NATS subject, a Kafka
  topic, an AMQP routing key.

  Its address is a **list of segments**, joined by the delimiter of whichever bus carries
  it. You do not write the delimiter, because it belongs to the transport rather than to
  your API:

      channel :ticket_events, ["helpdesk", "tickets", :id, "events"]

      # on MQTT → helpdesk/tickets/<id>/events
      # on NATS → helpdesk.tickets.<id>.events

  Segments interleave literals, fields and relationship paths, so an address can carry as
  much of the record's identity as you need:

      channel :comment_events, ["helpdesk", [:ticket, :organization_id], "tickets", [:ticket, :id], "comments", :id]

  Anything that is not a literal becomes a parameter: interpolated from the record when
  publishing, extracted from the concrete address when receiving. See `AshAsyncApi.Address`
  for the full segment grammar, and `AshAsyncApi.Channel.Parameter` for documenting them.

  This maps onto the AsyncAPI 3.0 [Channel
  Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#channelObject).
  """

  defstruct [
    :name,
    :segments,
    :delimiter,
    :title,
    :summary,
    :description,
    :external_docs,
    :__spark_metadata__,
    :__identifier__,
    servers: [],
    parameters: [],
    tags: [],
    bindings: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          segments: [AshAsyncApi.Address.segment()] | String.t() | nil,
          delimiter: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          external_docs: String.t() | nil,
          servers: [atom()],
          parameters: [AshAsyncApi.Channel.Parameter.t()],
          tags: [String.t()],
          bindings: map()
        }

  @schema [
    name: [
      type: :atom,
      required: true,
      doc: "The name of the channel, used to refer to it from operations."
    ],
    segments: [
      type: {:or, [{:list, :any}, :string, {:literal, nil}]},
      required: true,
      doc: """
      The address of the channel, as a list of segments joined by the delimiter of whichever
      bus carries it:

          ["helpdesk", "tickets", :id, "events"]

      A segment is a literal string, a field name, a relationship path
      (`[:organization, :id]`), or a `{name, path}` pair. Everything that is not a literal
      becomes a parameter.

      A plain string is also accepted, with `{braces}` marking the parameters, for addresses
      a segment list cannot express. `nil` means the address is unknown at design time.
      """
    ],
    delimiter: [
      type: :string,
      doc: """
      Overrides the delimiter for this channel. Normally the delimiter comes from the
      server carrying the channel — `/` on MQTT, `.` on NATS — and you should not set it.
      Needed only when a channel spans servers whose conventions disagree.
      """
    ],
    title: [
      type: :string,
      doc: "A human friendly title for the channel."
    ],
    summary: [
      type: :string,
      doc: "A short summary of the channel."
    ],
    description: [
      type: :string,
      doc: "A longer description of the channel. CommonMark is allowed."
    ],
    servers: [
      type: {:list, :atom},
      default: [],
      doc: """
      The names of the servers this channel is available on. When empty, the
      channel is available on every server.
      """
    ],
    tags: [
      type: {:list, :string},
      default: [],
      doc: "Tags for logical grouping of channels."
    ],
    external_docs: [
      type: :string,
      doc: "A URL pointing to external documentation for this channel."
    ],
    bindings: [
      type: :map,
      default: %{},
      doc: """
      Protocol specific information, keyed by protocol name, e.g
      `%{mqtt: %{qos: 1, retain: true}}`. Passed through to the transport when
      publishing, and rendered into the AsyncAPI document.
      """
    ]
  ]

  @doc false
  def schema, do: @schema

  @doc false
  def entity do
    %Spark.Dsl.Entity{
      name: :channel,
      describe: "Declare a channel that messages flow over.",
      examples: [
        """
        channel :ticket_events, ["helpdesk", "tickets", :id, "events"] do
          description "Lifecycle events for a single ticket"
          servers [:mqtt]
        end
        """,
        """
        channel :comment_events, ["helpdesk", [:ticket, :id], "comments"] do
          description "Comments, addressed by the ticket they belong to"
        end
        """,
        ~S|channel :audit, "helpdesk/audit"|
      ],
      args: [:name, :segments],
      target: __MODULE__,
      schema: @schema,
      identifier: :name,
      entities: [parameters: [AshAsyncApi.Channel.Parameter.entity()]]
    }
  end
end
