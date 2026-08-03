defmodule AshAsyncApi.Operation do
  @moduledoc """
  An operation binds an Ash action to a channel.

  There are two directions, named from the application's point of view — the same
  convention AsyncAPI 3.0 uses:

    * `publish` — the application **sends** a message. Running the action emits a
      message onto the channel. Renders as `action: send`.
    * `subscribe` — the application **receives** a message. A message arriving on
      the channel runs the action. Renders as `action: receive`.

  Maps onto the AsyncAPI 3.0 [Operation
  Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#operationObject).
  """

  defstruct [
    :name,
    :action,
    :channel,
    :direction,
    :resource,
    :message_name,
    :message_title,
    :content_type,
    :title,
    :summary,
    :description,
    :external_docs,
    :payload_fields,
    :except_fields,
    :transform,
    :filter,
    :reply_channel,
    :correlation_id,
    :actor,
    :tenant,
    :headers,
    :event_name,
    :__spark_metadata__,
    tags: [],
    bindings: %{},
    upsert?: false,
    action_type: nil,
    all?: false
  ]

  @type direction :: :send | :receive

  @type t :: %__MODULE__{
          name: atom(),
          action: atom(),
          channel: atom(),
          direction: direction(),
          resource: module() | nil,
          message_name: String.t() | nil,
          message_title: String.t() | nil,
          content_type: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          external_docs: String.t() | nil,
          payload_fields: [atom()] | nil,
          except_fields: [atom()] | nil,
          transform: (term(), term() -> term()) | nil,
          filter: (term(), term() -> boolean()) | nil,
          reply_channel: atom() | nil,
          correlation_id: String.t() | nil,
          actor: term(),
          tenant: term(),
          headers: map() | (term() -> map()) | nil,
          event_name: String.t() | nil,
          tags: [String.t()],
          bindings: map(),
          upsert?: boolean(),
          action_type: atom() | nil,
          all?: boolean()
        }

  @shared_schema [
    action: [
      type: :atom,
      required: true,
      doc: "The Ash action this operation is bound to."
    ],
    channel: [
      type: :atom,
      required: true,
      doc: "The name of the channel this operation acts on."
    ],
    name: [
      type: :atom,
      doc: """
      A unique name for the operation, used as the operation id in the generated
      AsyncAPI document. Defaults to `<type>_<action>`.
      """
    ],
    message_name: [
      type: :string,
      doc: """
      The name of the message this operation carries. Defaults to a camelized
      `<type>_<action>`, e.g `ticketOpened`.
      """
    ],
    message_title: [
      type: :string,
      doc: "A human friendly title for the message."
    ],
    content_type: [
      type: :string,
      doc:
        "The content type of the message payload. Defaults to the domain's `default_content_type`."
    ],
    title: [
      type: :string,
      doc: "A human friendly title for the operation."
    ],
    summary: [
      type: :string,
      doc: "A short summary of what the operation is about."
    ],
    description: [
      type: :string,
      doc: "A longer description of the operation. CommonMark is allowed."
    ],
    external_docs: [
      type: :string,
      doc: "A URL pointing to external documentation for this operation."
    ],
    tags: [
      type: {:list, :string},
      default: [],
      doc: "Tags for logically grouping operations."
    ],
    bindings: [
      type: :map,
      default: %{},
      doc: "Protocol specific information, keyed by protocol name."
    ],
    correlation_id: [
      type: :string,
      doc: """
      A runtime expression locating the correlation id, e.g
      `$message.header#/correlationId`. Used to tie replies back to requests.
      """
    ]
  ]

  @publish_options [
    payload_fields: [
      type: {:list, :atom},
      doc: """
      The fields to include in the message payload. Defaults to
      the resource's public attributes (minus `except_fields`).
      """
    ],
    except_fields: [
      type: {:list, :atom},
      default: [],
      doc: "Fields to exclude from the message payload."
    ],
    transform: [
      type: {:fun, 2},
      doc: """
      A function of `(payload, subject)` returning the final payload.
      Runs after field selection.
      """,
      snippet: "fn ${1:payload}, ${2:subject} -> $3 end"
    ],
    filter: [
      type: {:fun, 2},
      doc: """
      A function of `(record, notification_or_context)` returning a
      boolean. When it returns `false` the message is not published.
      """,
      snippet: "fn ${1:record}, ${2:context} -> $3 end"
    ],
    headers: [
      type: {:or, [:map, {:fun, 1}]},
      doc: """
      Static headers, or a one argument function of the subject
      returning a map of headers.
      """
    ],
    event_name: [
      type: :string,
      doc: """
      The event verb this operation represents, used by the `:_event` address
      segment. Defaults to `created`/`updated`/`destroyed` by action type, and to
      the action name otherwise. Past tense by convention, e.g `"deployed"`.
      """
    ],
    reply_channel: [
      type: :atom,
      doc: "The channel a reply to this message should be sent on."
    ]
  ]

  @publish_schema @shared_schema ++ @publish_options

  @publish_all_schema Keyword.delete(@shared_schema, :action) ++
                        [
                          action_type: [
                            type: {:in, [:create, :update, :destroy]},
                            required: true,
                            doc: """
                            The action type to publish. Every action of this type on the
                            resource publishes through this operation, expanded into one
                            operation per action when the routing table is built.
                            """
                          ]
                        ] ++ @publish_options

  @subscribe_schema @shared_schema ++
                      [
                        payload_fields: [
                          type: {:list, :atom},
                          doc: """
                          The message payload fields to accept as action input.
                          Defaults to the action's accepted attributes and arguments.
                          """
                        ],
                        except_fields: [
                          type: {:list, :atom},
                          default: [],
                          doc: "Payload fields to reject."
                        ],
                        transform: [
                          type: {:fun, 2},
                          doc: """
                          A function of `(payload, envelope)` returning the action input.
                          Runs before the action is called.
                          """,
                          snippet: "fn ${1:payload}, ${2:envelope} -> $3 end"
                        ],
                        filter: [
                          type: {:fun, 2},
                          doc: """
                          A function of `(payload, envelope)` returning a boolean. When
                          it returns `false` the message is acknowledged and dropped.
                          """,
                          snippet: "fn ${1:payload}, ${2:envelope} -> $3 end"
                        ],
                        actor: [
                          type: {:or, [:any, {:fun, 1}]},
                          doc: """
                          The actor to run the action as, or a one argument function of
                          the envelope returning the actor.
                          """
                        ],
                        tenant: [
                          type: {:or, [:any, {:fun, 1}]},
                          doc: """
                          The tenant to run the action in, or a one argument function of
                          the envelope returning the tenant.
                          """
                        ],
                        upsert?: [
                          type: :boolean,
                          default: false,
                          doc: "For create actions, run the action as an upsert."
                        ],
                        reply_channel: [
                          type: :atom,
                          doc: """
                          The channel the action's result should be published back on.
                          Turns the operation into a request/reply handler.
                          """
                        ]
                      ]

  @doc false
  def publish_schema, do: @publish_schema

  @doc false
  def publish_all_schema, do: @publish_all_schema

  @doc false
  def subscribe_schema, do: @subscribe_schema

  @doc false
  def publish_entity do
    %Spark.Dsl.Entity{
      name: :publish,
      describe: """
      Emit a message onto a channel when an action runs.

      Renders as an AsyncAPI operation with `action: send`.
      """,
      examples: [
        "publish :open, :ticket_events",
        """
        publish :open, :ticket_events do
          message_name "ticketOpened"
          payload_fields [:id, :subject, :status, :opened_at]
        end
        """
      ],
      args: [:action, :channel],
      target: __MODULE__,
      schema: @publish_schema,
      auto_set_fields: [direction: :send]
    }
  end

  @doc false
  def publish_all_entity do
    %Spark.Dsl.Entity{
      name: :publish_all,
      describe: """
      Emit a message onto a channel when any action of a type runs.

      Covers every create, update or destroy action on the resource — including
      custom-named ones — where `publish` names one action. When the routing table is
      built, a `publish_all` expands into one operation per matching action, so the
      generated document still lists every concrete operation. An action that also has
      its own `publish` on the same channel is left to that `publish`, rather than
      publishing twice.
      """,
      examples: [
        "publish_all :create, :events",
        """
        publish_all :update, :events do
          headers &MyApp.EventHeaders.build/1
        end
        """
      ],
      args: [:action_type, :channel],
      target: __MODULE__,
      schema: @publish_all_schema,
      auto_set_fields: [direction: :send, all?: true]
    }
  end

  @doc false
  def subscribe_entity do
    %Spark.Dsl.Entity{
      name: :subscribe,
      describe: """
      Run an action when a message arrives on a channel.

      Renders as an AsyncAPI operation with `action: receive`.
      """,
      examples: [
        "subscribe :open, :ticket_commands",
        """
        subscribe :assign, :ticket_commands do
          message_name "assignTicket"
          reply_channel :ticket_events
        end
        """
      ],
      args: [:action, :channel],
      target: __MODULE__,
      schema: @subscribe_schema,
      auto_set_fields: [direction: :receive]
    }
  end

  @doc "The AsyncAPI 3.0 `action` value for this operation."
  def async_api_action(%__MODULE__{direction: :send}), do: "send"
  def async_api_action(%__MODULE__{direction: :receive}), do: "receive"
end
