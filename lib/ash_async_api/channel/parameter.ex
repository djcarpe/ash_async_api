defmodule AshAsyncApi.Channel.Parameter do
  @moduledoc """
  A variable segment of a channel address.

  Given `channel :ticket_events, "helpdesk/tickets/{ticket_id}/events"`, the
  `ticket_id` parameter says where the value comes from when publishing, and
  what it means when receiving.

  Maps onto the AsyncAPI 3.0 [Parameter
  Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#parameterObject).
  """

  defstruct [
    :name,
    :description,
    :default,
    :location,
    :source,
    :__spark_metadata__,
    :__identifier__,
    enum: [],
    examples: []
  ]

  @type t :: %__MODULE__{
          name: atom(),
          description: String.t() | nil,
          default: String.t() | nil,
          location: String.t() | nil,
          source: atom() | [atom()] | (term() -> term()) | nil,
          enum: [String.t()],
          examples: [String.t()]
        }

  @schema [
    name: [
      type: :atom,
      required: true,
      doc: "The name of the parameter, as it appears in the channel address."
    ],
    source: [
      type: {:or, [:atom, {:list, :atom}, {:fun, 1}]},
      doc: """
      Where the value comes from when interpolating an outbound address: a field name, a
      relationship path like `[:organization, :id]`, or a one argument function of the
      record.

      With a segment-list address this is rarely needed — `["helpdesk", [:organization, :id]]`
      already says where the value comes from. It exists for string addresses, and for
      renaming a parameter whose derived name you do not like.
      """
    ],
    description: [
      type: :string,
      doc: "A description of the parameter. CommonMark is allowed."
    ],
    default: [
      type: :string,
      doc: "The default value to use when no value is supplied."
    ],
    enum: [
      type: {:list, :string},
      default: [],
      doc: "An enumeration of the values this parameter may take."
    ],
    examples: [
      type: {:list, :string},
      default: [],
      doc: "Example values for this parameter."
    ],
    location: [
      type: :string,
      doc: """
      A runtime expression describing where the value is defined, e.g
      `$message.payload#/ticketId`.
      """
    ]
  ]

  @doc false
  def schema, do: @schema

  @doc false
  def entity do
    %Spark.Dsl.Entity{
      name: :parameter,
      describe: "Describe a `{templated}` segment of the channel address.",
      examples: [
        """
        parameter :ticket_id do
          description "The id of the ticket"
        end
        """,
        "parameter :tenant, source: :organization_id"
      ],
      args: [:name],
      target: __MODULE__,
      schema: @schema,
      identifier: :name
    }
  end

  @doc """
  The explicit source of a parameter, or `nil` when it does not declare one.

  `nil` means "use the path from the address", which is almost always right — the segment
  `[:organization, :id]` already says where the value comes from. This deliberately does not
  fall back to the parameter's *name*: a `parameter` block that only carries a description
  must not change where the value is read from.
  """
  @spec source(t()) :: atom() | [atom()] | (term() -> term()) | nil
  def source(%__MODULE__{source: source}), do: source
end
