defmodule AshAsyncApi.Resource do
  @moduledoc """
  The extension for adding AsyncAPI behaviour to an `Ash.Resource`.

      defmodule Helpdesk.Support.Ticket do
        use Ash.Resource,
          domain: Helpdesk.Support,
          extensions: [AshAsyncApi.Resource]

        async_api do
          type "ticket"

          channels do
            channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
              parameter :ticket_id, source: :id
            end
          end

          operations do
            publish :open, :ticket_events
            publish :close, :ticket_events
            subscribe :open, :ticket_commands
          end
        end
      end

  Channels and operations can equally be declared on the domain — see
  `AshAsyncApi.Domain`. Declaring them here keeps a resource's messaging
  co-located with it; declaring them on the domain lets one place describe a whole
  API surface.
  """

  @channels %Spark.Dsl.Section{
    name: :channels,
    describe: """
    The channels this resource's messages flow over.

    A channel declared here is scoped to this resource and can be referenced by
    name from this resource's operations.
    """,
    examples: [
      """
      channels do
        channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
          description "Lifecycle events for a single ticket"
          parameter :ticket_id, source: :id
        end
      end
      """
    ],
    entities: [AshAsyncApi.Channel.entity()]
  }

  @operations %Spark.Dsl.Section{
    name: :operations,
    describe: """
    The operations that bind this resource's actions to channels.

    `publish` sends a message when an action runs (`publish_all` for every action of a
    type); `subscribe` runs an action when a message arrives.
    """,
    examples: [
      """
      operations do
        publish :open, :ticket_events do
          message_name "ticketOpened"
        end

        publish_all :update, :ticket_events

        subscribe :assign, :ticket_commands
      end
      """
    ],
    entities: [
      AshAsyncApi.Operation.publish_entity(),
      AshAsyncApi.Operation.publish_all_entity(),
      AshAsyncApi.Operation.subscribe_entity()
    ]
  }

  @async_api %Spark.Dsl.Section{
    name: :async_api,
    describe: "Configure the AsyncAPI behaviour of this resource.",
    examples: [
      """
      async_api do
        type "ticket"

        channels do
          channel :ticket_events, "helpdesk/tickets/{ticket_id}/events"
        end

        operations do
          publish :open, :ticket_events
        end
      end
      """
    ],
    schema: [
      type: [
        type: :string,
        doc: """
        The AsyncAPI type name for this resource. Used to name generated messages
        and component schemas, and to derive default operation ids. Defaults to
        the resource's short module name in snake case.
        """
      ],
      segment_naming: [
        type: {:or, [{:in, [:snake, :camel, :pascal]}, {:mfa_or_fun, 1}]},
        doc: """
        How the `:_domain` and `:_resource` address segments render for this
        resource's channels, overriding the domain's `segment_naming` and any
        application config. `:snake` snake-cases the type, `:camel` lower-camelizes
        it, `:pascal` upper-camelizes it, and a one-argument function or
        `{module, function, args}` receives the domain or resource module and
        returns the segment string. When unset, the domain's setting (or the
        `:ash_async_api_segment_naming` application config, then `:snake`) applies.
        """
      ],
      default_content_type: [
        type: :string,
        doc: """
        The content type for this resource's messages. Defaults to the domain's
        `default_content_type`.
        """
      ],
      include_nil_values?: [
        type: :boolean,
        default: false,
        doc: "Whether `nil` payload fields are included in published messages."
      ],
      hide_fields: [
        type: {:list, :atom},
        default: [],
        doc: """
        Fields to omit from every generated payload schema and published message.
        Applied on top of any `payload_fields` on an operation, so a hidden field
        stays hidden.
        """
      ],
      show_fields: [
        type: {:list, :atom},
        doc: """
        The only fields eligible to appear in payloads. When set, acts as an
        allow-list; `hide_fields` still subtracts from it.
        """
      ],
      derive_payload_schema?: [
        type: :boolean,
        default: true,
        doc: """
        Whether to derive JSON Schema payloads from the resource's attributes and
        action inputs. When `false`, payload schemas render as a permissive
        `{"type": "object"}`.
        """
      ],
      publish_on_notification?: [
        type: :boolean,
        default: true,
        doc: """
        Whether `publish` operations fire automatically off Ash notifications. When
        `false`, publishing is manual via `AshAsyncApi.publish/3`.
        """
      ],
      tags: [
        type: {:list, :string},
        default: [],
        doc: "Tags applied to every operation and message derived from this resource."
      ]
    ],
    sections: [@channels, @operations]
  }

  @transformers [
    AshAsyncApi.Resource.Transformers.SetType,
    AshAsyncApi.Resource.Transformers.DefaultOperationNames,
    AshAsyncApi.Resource.Transformers.SetupNotifier
  ]

  @verifiers [
    AshAsyncApi.Resource.Verifiers.VerifyChannels,
    AshAsyncApi.Resource.Verifiers.VerifyActions,
    AshAsyncApi.Resource.Verifiers.VerifyFieldReferences,
    AshAsyncApi.Resource.Verifiers.VerifyOperationNames
  ]

  @sections [@async_api]

  use Spark.Dsl.Extension,
    sections: @sections,
    transformers: @transformers,
    verifiers: @verifiers,
    module_prefix: AshAsyncApi.Resource.Dsl

  @doc false
  def channels_section, do: @channels

  @doc false
  def operations_section, do: @operations
end
