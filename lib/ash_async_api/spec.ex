defmodule AshAsyncApi.Spec do
  @moduledoc """
  Generates the [AsyncAPI 3.0.0](https://www.asyncapi.com/docs/reference/specification/v3.0.0)
  document for a router.

      MyApp.AsyncApiRouter.spec()
      MyApp.AsyncApiRouter.spec_json()
      MyApp.AsyncApiRouter.spec_yaml()

  or from the command line:

      mix ash_async_api.spec --router MyApp.AsyncApiRouter --output asyncapi.json

  Messages are emitted once into `components.messages` and referenced from channels,
  and payload schemas go into `components.schemas`, which is what tooling expects and
  what keeps a document with many channels from repeating itself.

  ## Structure

  The document mirrors the DSL closely:

  | AsyncAPI | Comes from |
  | -------- | ---------- |
  | `info` | the domain's `info` block |
  | `servers` | the domain's `servers` |
  | `channels` | `channels` on domains and resources |
  | `channels.*.messages` | the operations on that channel |
  | `operations` | `publish` (`send`) and `subscribe` (`receive`) |
  | `components.schemas` | Ash attributes and action inputs |

  ## Multiple domains

  A router serving several domains produces one document. `info` comes from the first
  domain unless you pass `:info`; servers and channels are merged.
  """

  alias AshAsyncApi.Router.Table.ResolvedChannel

  @asyncapi_version "3.0.0"

  @doc """
  Generate the AsyncAPI document for a router, as a map with string keys.

  ## Options

    * `:info` — a map merged over the derived `info` object, for overriding the title
      or version at generation time (e.g from `Application.spec(:my_app, :vsn)`).
    * `:servers` — restrict the document to these server names, *and* to the channels
      reachable on them. For publishing a document that describes only the public brokers.
    * `:include_local?` — whether servers using `AshAsyncApi.Transport.Local` appear in
      `servers`. Defaults to `false`, since in-cluster delivery is an implementation detail
      rather than something a consumer can connect to. This affects only the `servers`
      section: the channels and their message schemas are the API description and always
      appear.
    * `:id` — overrides the domain's `id`.
  """
  @spec generate(module(), keyword()) :: map()
  def generate(router, opts \\ []) do
    table = router.__ash_async_api__()
    servers = servers(table, opts)
    channels = channels(table, servers, opts)
    operations = operations(table, channels)

    %{
      "asyncapi" => @asyncapi_version,
      "info" => info(table, opts)
    }
    |> maybe_put("id", opts[:id] || document_id(table))
    |> maybe_put("defaultContentType", default_content_type(table))
    |> maybe_put("servers", render_servers(servers))
    |> maybe_put("channels", render_channels(channels, servers))
    |> maybe_put("operations", render_operations(operations))
    |> maybe_put("components", components(channels, operations, table))
  end

  @doc """
  The document as pretty-printed JSON.
  """
  @spec to_json(module(), keyword()) :: String.t()
  def to_json(router, opts \\ []) do
    router |> generate(opts) |> Jason.encode!(pretty: true)
  end

  @doc """
  The document as YAML. Requires the optional `:ymlr` dependency.
  """
  @spec to_yaml(module(), keyword()) :: String.t()
  def to_yaml(router, opts \\ []) do
    unless Code.ensure_loaded?(Ymlr) do
      raise """
      Generating YAML requires the :ymlr dependency. Add it to your mix.exs:

          {:ymlr, "~> 5.0"}
      """
    end

    router |> generate(opts) |> Ymlr.document!()
  end

  @doc """
  The AsyncAPI specification version this module targets.
  """
  @spec version() :: String.t()
  def version, do: @asyncapi_version

  defp info(table, opts) do
    domain = hd(table.domains)
    info = AshAsyncApi.Domain.Info.info(domain)

    %{"title" => info.title, "version" => info.version}
    |> maybe_put("description", info.description)
    |> maybe_put("termsOfService", info.terms_of_service)
    |> maybe_put("contact", contact(info))
    |> maybe_put("license", license(info))
    |> maybe_put("externalDocs", external_docs(info.external_docs))
    |> maybe_put("tags", render_tags(info.tags))
    |> Map.merge(stringify_keys(opts[:info] || %{}))
  end

  defp contact(info) do
    %{}
    |> maybe_put("name", info.contact_name)
    |> maybe_put("url", info.contact_url)
    |> maybe_put("email", info.contact_email)
    |> presence()
  end

  defp license(%{license_name: nil}), do: nil

  defp license(info) do
    %{"name" => info.license_name} |> maybe_put("url", info.license_url)
  end

  defp document_id(table) do
    table.domains |> hd() |> AshAsyncApi.Domain.Info.id()
  end

  defp default_content_type(table) do
    table.domains |> hd() |> AshAsyncApi.Domain.Info.default_content_type()
  end

  defp servers(table, opts) do
    include_local? = Keyword.get(opts, :include_local?, false)
    only = opts[:servers] && MapSet.new(List.wrap(opts[:servers]))

    table.servers
    |> Enum.filter(fn {name, {_domain, server}} ->
      (is_nil(only) or MapSet.member?(only, name)) and
        (include_local? or server.transport != AshAsyncApi.Transport.Local)
    end)
    |> Enum.map(fn {_name, {_domain, server}} -> server end)
    |> Enum.sort_by(& &1.name)
  end

  # Channels and their messages are the API description proper, so they stay in the
  # document even when the server carrying them was filtered out of `servers` — hiding
  # the in-cluster server should not hide what the messages look like. Only an explicit
  # `:servers` option, which means "describe just these brokers", narrows the channels.
  defp channels(table, servers, opts) do
    channels = Enum.filter(table.channels, &(&1.operations != []))

    channels =
      if opts[:servers] do
        names = MapSet.new(servers, & &1.name)

        Enum.filter(channels, fn channel ->
          Enum.any?(channel.servers, &MapSet.member?(names, &1))
        end)
      else
        channels
      end

    Enum.sort_by(channels, & &1.key)
  end

  defp operations(_table, channels) do
    channels
    |> Enum.flat_map(& &1.operations)
    |> Enum.sort_by(& &1.name)
  end

  defp render_servers([]), do: nil

  defp render_servers(servers) do
    Map.new(servers, fn server ->
      {to_string(server.name),
       %{"host" => server.host, "protocol" => to_string(server.protocol)}
       |> maybe_put("protocolVersion", server.protocol_version)
       |> maybe_put("pathname", server.pathname)
       |> maybe_put("title", server.title)
       |> maybe_put("summary", server.summary)
       |> maybe_put("description", server.description)
       |> maybe_put("variables", presence(stringify_keys(server.variables)))
       |> maybe_put("security", presence(server.security))
       |> maybe_put("tags", render_tags(server.tags))
       |> maybe_put("externalDocs", external_docs(server.external_docs))
       |> maybe_put("bindings", presence(stringify_keys(server.bindings)))}
    end)
  end

  defp render_channels([], _servers), do: nil

  defp render_channels(channels, servers) do
    server_names = MapSet.new(servers, & &1.name)

    Map.new(channels, fn channel ->
      {to_string(channel.key), render_channel(channel, server_names)}
    end)
  end

  defp render_channel(channel, server_names) do
    %{}
    |> maybe_put("address", channel.address)
    |> maybe_put("title", channel.channel.title)
    |> maybe_put("summary", channel.channel.summary)
    |> maybe_put("description", channel.channel.description)
    |> maybe_put("servers", channel_server_refs(channel, server_names))
    |> maybe_put("parameters", parameters(channel))
    |> maybe_put("messages", channel_messages(channel))
    |> maybe_put("tags", render_tags(channel.channel.tags))
    |> maybe_put("externalDocs", external_docs(channel.channel.external_docs))
    |> maybe_put("bindings", presence(stringify_keys(channel.channel.bindings)))
  end

  # A channel listing every server is the same as listing none, and the spec reads
  # better without the noise.
  defp channel_server_refs(channel, server_names) do
    refs = Enum.filter(channel.servers, &MapSet.member?(server_names, &1))

    if length(refs) == MapSet.size(server_names) do
      nil
    else
      presence(Enum.map(refs, &%{"$ref" => "#/servers/#{&1}"}))
    end
  end

  defp channel_messages(channel) do
    channel.operations
    |> Enum.map(&{&1.message_name, %{"$ref" => "#/components/messages/#{&1.message_name}"}})
    |> Map.new()
    |> presence()
  end

  # Every `{parameter}` in the address must appear here, whether or not the DSL
  # described it — the spec requires it, and tooling uses it to build addresses.
  defp parameters(%ResolvedChannel{compiled: nil}), do: nil

  defp parameters(%ResolvedChannel{compiled: compiled} = channel) do
    compiled.params
    |> Map.new(fn name ->
      {to_string(name), render_parameter(ResolvedChannel.parameter(channel, name))}
    end)
    |> presence()
  end

  defp render_parameter(nil), do: %{}

  defp render_parameter(parameter) do
    %{}
    |> maybe_put("description", parameter.description)
    |> maybe_put("default", parameter.default)
    |> maybe_put("enum", presence(parameter.enum))
    |> maybe_put("examples", presence(parameter.examples))
    |> maybe_put("location", parameter.location)
  end

  defp render_operations([]), do: nil

  defp render_operations(operations) do
    Map.new(operations, fn operation ->
      {to_string(operation.name), render_operation(operation)}
    end)
  end

  defp render_operation(operation) do
    %{
      "action" => AshAsyncApi.Operation.async_api_action(operation.operation),
      "channel" => %{"$ref" => "#/channels/#{operation.channel_key}"},
      "messages" => [
        %{"$ref" => "#/channels/#{operation.channel_key}/messages/#{operation.message_name}"}
      ]
    }
    |> maybe_put("title", operation.operation.title)
    |> maybe_put("summary", operation.operation.summary)
    |> maybe_put("description", description(operation))
    |> maybe_put("tags", render_tags(operation_tags(operation)))
    |> maybe_put("externalDocs", external_docs(operation.operation.external_docs))
    |> maybe_put("bindings", presence(stringify_keys(operation.operation.bindings)))
    |> maybe_put("reply", reply(operation))
  end

  # The action's own documentation is the best description available, and repeating it
  # in the DSL just to get it into the spec would be silly.
  defp description(operation) do
    operation.operation.description || action_description(operation)
  end

  defp action_description(%{resource: nil}), do: nil

  defp action_description(%{resource: resource, action: action_name}) do
    case Ash.Resource.Info.action(resource, action_name) do
      %{description: description} when is_binary(description) -> description
      _ -> nil
    end
  end

  defp operation_tags(operation) do
    resource_tags =
      case operation.resource do
        nil -> []
        resource -> AshAsyncApi.Resource.Info.tags(resource)
      end

    Enum.uniq(operation.operation.tags ++ resource_tags)
  end

  defp reply(%{reply_channel_key: nil}), do: nil

  defp reply(%{reply_channel_key: key}) do
    %{"channel" => %{"$ref" => "#/channels/#{key}"}}
  end

  defp components(channels, operations, table) do
    %{}
    |> maybe_put("messages", messages(operations))
    |> maybe_put("schemas", schemas(operations))
    |> maybe_put("securitySchemes", security_schemes(table))
    |> then(fn components ->
      if channels == [], do: presence(components), else: presence(components)
    end)
  end

  defp messages(operations) do
    operations
    |> Map.new(fn operation -> {operation.message_name, render_message(operation)} end)
    |> presence()
  end

  defp render_message(operation) do
    %{
      "name" => operation.message_name,
      "payload" => %{"$ref" => "#/components/schemas/#{schema_name(operation)}"},
      "contentType" => operation.content_type
    }
    |> maybe_put("title", operation.operation.message_title)
    |> maybe_put("summary", operation.operation.summary)
    |> maybe_put("description", description(operation))
    |> maybe_put("correlationId", correlation_id(operation))
    |> maybe_put("tags", render_tags(operation_tags(operation)))
    |> maybe_put("bindings", presence(stringify_keys(operation.operation.bindings)))
  end

  defp correlation_id(%{operation: %{correlation_id: nil}}), do: nil

  defp correlation_id(%{operation: %{correlation_id: location}}) do
    %{"location" => location}
  end

  defp schemas(operations) do
    operations
    |> Map.new(fn operation -> {schema_name(operation), payload_schema(operation)} end)
    |> presence()
  end

  defp payload_schema(%{direction: :send} = operation),
    do: AshAsyncApi.Spec.Schema.for_send(operation)

  defp payload_schema(%{direction: :receive} = operation),
    do: AshAsyncApi.Spec.Schema.for_receive(operation)

  defp schema_name(operation), do: "#{operation.message_name}Payload"

  defp security_schemes(table) do
    table.domains
    |> Enum.reduce(%{}, fn domain, acc ->
      Map.merge(acc, stringify_keys(AshAsyncApi.Domain.Info.security_schemes(domain)))
    end)
    |> presence()
  end

  defp render_tags([]), do: nil
  defp render_tags(tags), do: Enum.map(tags, &%{"name" => &1})

  defp external_docs(nil), do: nil
  defp external_docs(url), do: %{"url" => url}

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, %{} = value} when not is_struct(value) -> {to_string(key), stringify_keys(value)}
      {key, value} -> {to_string(key), value}
    end)
  end

  defp stringify_keys(other), do: other

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp presence(empty) when empty == %{}, do: nil
  defp presence([]), do: nil
  defp presence(value), do: value
end
