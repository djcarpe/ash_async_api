defmodule AshAsyncApi.Domain do
  @moduledoc """
  The extension for adding AsyncAPI behaviour to an `Ash.Domain`.

  The domain is where the document-level metadata lives — `info`, `servers`,
  `security` — and it can also declare channels and operations on behalf of its
  resources, which keeps the whole API surface describable in one place:

      defmodule Helpdesk.Support do
        use Ash.Domain, extensions: [AshAsyncApi.Domain]

        async_api do
          info do
            title "Helpdesk Events"
            version "1.0.0"
          end

          servers do
            server :mqtt, "broker.example.com:1883" do
              protocol :mqtt
              transport AshAsyncApi.Transport.Mqtt
            end
          end

          channels do
            channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
              servers [:mqtt]
            end
          end

          operations do
            publish Helpdesk.Support.Ticket, :open, :ticket_events
            subscribe Helpdesk.Support.Ticket, :assign, :ticket_commands
          end
        end
      end
  """

  @info %Spark.Dsl.Section{
    name: :info,
    describe: """
    Document metadata, rendered as the AsyncAPI [Info
    Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#infoObject).
    """,
    examples: [
      """
      info do
        title "Helpdesk Events"
        version "1.0.0"
        description "Everything that happens to a ticket"
      end
      """
    ],
    schema: [
      title: [
        type: :string,
        doc: "The title of the application. Defaults to the domain's short name."
      ],
      version: [
        type: :string,
        default: "1.0.0",
        doc: "The version of this application's API."
      ],
      description: [
        type: :string,
        doc: "A description of the application. CommonMark is allowed."
      ],
      terms_of_service: [
        type: :string,
        doc: "A URL to the Terms of Service for the API."
      ],
      contact_name: [
        type: :string,
        doc: "The identifying name of the contact person or organization."
      ],
      contact_url: [
        type: :string,
        doc: "A URL pointing to the contact information."
      ],
      contact_email: [
        type: :string,
        doc: "The email address of the contact person or organization."
      ],
      license_name: [
        type: :string,
        doc: "The license name used for the API."
      ],
      license_url: [
        type: :string,
        doc: "A URL to the license used for the API."
      ],
      external_docs: [
        type: :string,
        doc: "A URL pointing to external documentation for the application."
      ],
      tags: [
        type: {:list, :string},
        default: [],
        doc: "Tags for logical grouping of the application."
      ]
    ]
  }

  @servers %Spark.Dsl.Section{
    name: :servers,
    describe: """
    The brokers this application connects to, and the transports that talk to them.
    """,
    examples: [
      """
      servers do
        server :mqtt, "broker.example.com:1883" do
          protocol :mqtt
          transport AshAsyncApi.Transport.Mqtt
          transport_opts [client_id: "helpdesk"]
        end

        server :local, "erlang-cluster" do
          protocol :erlang
          transport AshAsyncApi.Transport.Local
        end
      end
      """
    ],
    entities: [AshAsyncApi.Server.entity()]
  }

  @channels %Spark.Dsl.Section{
    name: :channels,
    describe: """
    Channels shared across the domain.

    Any resource in the domain can reference these by name. A channel declared on a
    resource shadows a domain channel with the same name.
    """,
    examples: [
      """
      channels do
        channel :ticket_events, "helpdesk/tickets/{ticket_id}/events" do
          servers [:mqtt]
        end
      end
      """
    ],
    entities: [AshAsyncApi.Channel.entity()]
  }

  # Domain-level operations need to say which resource they belong to, so both
  # entities get `:resource` prepended to their positional args.
  @domain_operation_entities [
                               AshAsyncApi.Operation.publish_entity(),
                               AshAsyncApi.Operation.publish_all_entity(),
                               AshAsyncApi.Operation.subscribe_entity()
                             ]
                             |> Enum.map(fn entity ->
                               %{
                                 entity
                                 | args: [:resource | entity.args],
                                   schema:
                                     Keyword.put(entity.schema, :resource,
                                       type: {:spark, Ash.Resource},
                                       required: true,
                                       doc:
                                         "The resource whose action this operation is bound to."
                                     )
                               }
                             end)

  @operations %Spark.Dsl.Section{
    name: :operations,
    describe: """
    Operations declared on behalf of the domain's resources.

    Identical to the resource-level `operations` section, except that each entry
    names the resource it applies to first.
    """,
    examples: [
      """
      operations do
        publish Helpdesk.Support.Ticket, :open, :ticket_events do
          message_name "ticketOpened"
        end

        subscribe Helpdesk.Support.Ticket, :assign, :ticket_commands
      end
      """
    ],
    entities: @domain_operation_entities
  }

  @async_api %Spark.Dsl.Section{
    name: :async_api,
    describe: "Configure the AsyncAPI document and messaging for this domain.",
    examples: [
      """
      async_api do
        info do
          title "Helpdesk Events"
          version "1.0.0"
        end

        servers do
          server :mqtt, "broker.example.com:1883" do
            protocol :mqtt
            transport AshAsyncApi.Transport.Mqtt
          end
        end
      end
      """
    ],
    schema: [
      id: [
        type: :string,
        doc: """
        A URI identifying the application, e.g `urn:com:example:helpdesk`. Rendered
        as the AsyncAPI document's `id`.
        """
      ],
      type: [
        type: :string,
        doc: """
        The name the `:_domain` address segment resolves to for this domain's
        channels. Defaults to the domain's short module name in snake case.
        """
      ],
      segment_naming: [
        type: {:or, [{:in, [:snake, :camel, :pascal]}, {:mfa_or_fun, 1}]},
        doc: """
        How the `:_domain` and `:_resource` address segments render for channels in
        this domain. `:snake` snake-cases the domain/resource `type`, `:camel`
        lower-camelizes it (`work_element` becomes `workElement`), `:pascal`
        upper-camelizes it (`WorkElement`), and a one-argument function or
        `{module, function, args}` receives the domain or resource module and
        returns the segment string, taking full control.

        When unset, application config decides —
        `config :my_app, :ash_async_api_segment_naming, :camel` on the domain's
        `otp_app` — and `:snake` is the final default. A resource-level
        `segment_naming` overrides all of this for that resource's channels.
        """
      ],
      default_content_type: [
        type: :string,
        default: "application/json",
        doc: "The content type used for messages that do not specify their own."
      ],
      default_server: [
        type: :atom,
        doc: """
        The server used by channels that do not name any. Defaults to the only
        server when exactly one is declared.
        """
      ],
      default_delimiter: [
        type: :string,
        doc: """
        The delimiter joining address segments for channels in this domain, overriding what
        the servers' protocols imply. Set this only to settle a conflict — normally the bus
        decides, which is what lets one channel declaration work on MQTT and NATS alike.
        """
      ],
      security_schemes: [
        type: :map,
        default: %{},
        doc: """
        Reusable security schemes, keyed by name, rendered into
        `components.securitySchemes`.
        """
      ],
      trace?: [
        type: :boolean,
        default: true,
        doc: "Whether to emit `:telemetry` spans for published and received messages."
      ]
    ],
    sections: [@info, @servers, @channels, @operations]
  }

  @transformers [
    AshAsyncApi.Domain.Transformers.SetDefaultServer,
    AshAsyncApi.Domain.Transformers.DefaultDomainOperationNames
  ]

  @verifiers [
    AshAsyncApi.Domain.Verifiers.VerifyServers,
    AshAsyncApi.Domain.Verifiers.VerifyChannelReferences,
    AshAsyncApi.Domain.Verifiers.VerifyTransports
  ]

  @sections [@async_api]

  # Without a prefix, Spark would generate `AshAsyncApi.Domain.Info` for the `info`
  # section, colliding with the introspection module of the same name.
  use Spark.Dsl.Extension,
    sections: @sections,
    transformers: @transformers,
    verifiers: @verifiers,
    module_prefix: AshAsyncApi.Domain.Dsl

  @doc false
  def servers_section, do: @servers

  @doc false
  def channels_section, do: @channels

  @doc false
  def operations_section, do: @operations
end
