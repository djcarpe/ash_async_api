defmodule AshAsyncApi.Channel do
  @moduledoc """
  A channel is an addressable conduit for messages — an MQTT topic, a NATS
  subject, a Kafka topic, an AMQP routing key.

  Channels are declared with a *template* address. Segments wrapped in braces
  are `AshAsyncApi.Channel.Parameter`s that get interpolated from the record or
  message being published, and extracted from the concrete address on the way in:

      channel :ticket_events, "helpdesk/tickets/{ticket_id}/events"

  This maps directly onto the AsyncAPI 3.0 [Channel
  Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#channelObject).
  """

  defstruct [
    :name,
    :address,
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
          address: String.t() | nil,
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
    address: [
      type: {:or, [:string, {:literal, nil}]},
      required: true,
      doc: """
      The address of the channel, i.e the topic/subject/routing key. May contain
      `{parameter}` placeholders. `nil` means the address is unknown at design
      time (see the AsyncAPI spec).
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
        channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
          description "Lifecycle events for a single ticket"
          servers [:mqtt]

          parameter :ticket_id do
            description "The id of the ticket"
          end
        end
        """
      ],
      args: [:name, :address],
      target: __MODULE__,
      schema: @schema,
      identifier: :name,
      entities: [parameters: [AshAsyncApi.Channel.Parameter.entity()]]
    }
  end
end
