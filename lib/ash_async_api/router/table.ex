defmodule AshAsyncApi.Router.Table do
  @moduledoc """
  The resolved, indexed view of everything a router knows about.

  Channels and operations can be declared on a domain or on a resource, and
  operations reference channels by bare name. Resolving all of that on every
  published message would be wasteful, so a router builds this table once and caches
  it in `:persistent_term`.

  The table also normalizes a wrinkle in the DSL: AsyncAPI identifies a channel by
  its *address*, while the DSL identifies it by name, and two resources may
  legitimately use the same name for different addresses. Building the table assigns
  each distinct address one unique `key`, which is what ends up in the AsyncAPI
  document and in `AshAsyncApi.Envelope.channel`.
  """

  alias AshAsyncApi.Router.Table.ResolvedChannel
  alias AshAsyncApi.Router.Table.ResolvedOperation

  defstruct router: nil,
            domains: [],
            servers: %{},
            channels: [],
            operations: [],
            inbound: [],
            outbound: %{},
            by_key: %{}

  @type t :: %__MODULE__{
          router: module(),
          domains: [module()],
          servers: %{atom() => {module(), AshAsyncApi.Server.t()}},
          channels: [ResolvedChannel.t()],
          operations: [ResolvedOperation.t()],
          inbound: [ResolvedChannel.t()],
          outbound: %{{module(), atom(), atom()} => [ResolvedOperation.t()]},
          by_key: %{atom() => ResolvedChannel.t()}
        }

  @doc """
  Build the table for a router over the given domains.

  Raises `AshAsyncApi.Error.UnknownChannel` when an operation references a channel
  that neither its resource nor its domain declares — the check that
  `AshAsyncApi.Domain.Verifiers.VerifyChannelReferences` cannot always make at
  compile time.
  """
  @spec build(module(), [module()]) :: t()
  def build(router, domains) do
    domains = List.wrap(domains)
    servers = collect_servers(domains)
    declarations = collect_declarations(domains)
    operations = resolve_operations(declarations, resolve_channels(declarations, servers))

    # Attach each channel's operations only after operations are resolved, so the
    # resolved channels the table hands out are complete.
    channels =
      declarations
      |> resolve_channels(servers)
      |> Enum.map(fn channel ->
        %{channel | operations: Enum.filter(operations, &(&1.channel_key == channel.key))}
      end)

    %__MODULE__{
      router: router,
      domains: domains,
      servers: servers,
      channels: channels,
      operations: operations,
      by_key: Map.new(channels, &{&1.key, &1}),
      inbound: Enum.filter(channels, &has_direction?(&1, :receive)),
      outbound: index_outbound(operations)
    }
  end

  @doc """
  The resolved channel for a key.
  """
  @spec channel(t(), atom()) :: ResolvedChannel.t() | nil
  def channel(%__MODULE__{by_key: by_key}, key), do: Map.get(by_key, key)

  @doc """
  The resolved channel a resource's operation targets.

  This is the lookup used when publishing: an operation is identified by resource,
  action and direction, and the same action may feed more than one channel.
  """
  @spec operations_for(t(), module(), atom(), :send | :receive) :: [ResolvedOperation.t()]
  def operations_for(%__MODULE__{outbound: outbound}, resource, action, direction) do
    Map.get(outbound, {resource, action, direction}, [])
  end

  @doc """
  The channels reachable on a given server, for inbound subscription setup.
  """
  @spec channels_for_server(t(), atom()) :: [ResolvedChannel.t()]
  def channels_for_server(%__MODULE__{channels: channels}, server_name) do
    Enum.filter(channels, &(server_name in &1.servers))
  end

  @doc """
  Find the channels whose address template matches a concrete address.

  Returns `{channel, params}` pairs. More than one channel can match — a catch-all
  and a specific channel may both cover an address — and every match is delivered to.
  """
  @spec match_address(t(), String.t(), atom() | nil) ::
          [{ResolvedChannel.t(), %{atom() => String.t()}}]
  def match_address(%__MODULE__{inbound: inbound}, address, server_name \\ nil) do
    inbound
    |> Enum.filter(fn channel ->
      is_nil(server_name) or server_name in channel.servers
    end)
    |> Enum.flat_map(fn channel ->
      case AshAsyncApi.Address.match(channel.compiled, address) do
        {:ok, params} -> [{channel, params}]
        :error -> []
      end
    end)
  end

  defp collect_servers(domains) do
    for domain <- domains,
        server <- AshAsyncApi.Domain.Info.servers(domain),
        into: %{} do
      {server.name, {domain, server}}
    end
  end

  # A declaration is one (scope, channel-or-operation) pair, where scope carries the
  # domain and, for resource-level declarations, the resource. Keeping them together
  # is what lets a resource channel shadow a domain channel of the same name.
  defp collect_declarations(domains) do
    Enum.flat_map(domains, fn domain ->
      domain_scope = %{domain: domain, resource: nil}

      domain_declarations = [
        {domain_scope, AshAsyncApi.Domain.Info.channels(domain),
         AshAsyncApi.Domain.Info.operations(domain)}
      ]

      resource_declarations =
        domain
        |> Ash.Domain.Info.resources()
        |> Enum.filter(&AshAsyncApi.Resource.Info.async_api?/1)
        |> Enum.map(fn resource ->
          {%{domain: domain, resource: resource}, AshAsyncApi.Resource.Info.channels(resource),
           AshAsyncApi.Resource.Info.operations(resource)}
        end)

      domain_declarations ++ resource_declarations
    end)
  end

  defp resolve_channels(declarations, servers) do
    server_names = Map.keys(servers)

    declarations
    |> Enum.flat_map(fn {scope, channels, _operations} ->
      Enum.map(channels, &{scope, &1})
    end)
    # AsyncAPI identifies a channel by its address, so declarations that agree on
    # name *and* segments are one channel, however many resources declared it.
    |> Enum.uniq_by(fn {_scope, channel} -> {channel.name, channel.segments} end)
    |> assign_keys()
    |> Enum.map(fn {key, scope, channel} ->
      channel_servers = resolve_servers(channel, scope.domain, server_names)
      delimiter = resolve_delimiter(channel, scope.domain, channel_servers, servers)

      compiled =
        channel.segments && AshAsyncApi.Address.compile(channel.segments, delimiter: delimiter)

      %ResolvedChannel{
        key: key,
        name: channel.name,
        address: compiled && compiled.template,
        compiled: compiled,
        delimiter: delimiter,
        channel: channel,
        domain: scope.domain,
        resource: scope.resource,
        servers: channel_servers
      }
    end)
  end

  # The delimiter comes from the bus, which is the whole point of declaring addresses as
  # segment lists: the same channel is `helpdesk/tickets/1` on MQTT and `helpdesk.tickets.1`
  # on NATS without being written twice.
  defp resolve_delimiter(%{delimiter: delimiter}, _domain, _channel_servers, _servers)
       when is_binary(delimiter),
       do: delimiter

  defp resolve_delimiter(channel, domain, channel_servers, servers) do
    delimiters =
      channel_servers
      |> Enum.flat_map(fn name ->
        case Map.get(servers, name) do
          {_domain, server} -> [AshAsyncApi.Server.delimiter(server)]
          nil -> []
        end
      end)
      |> Enum.uniq()

    case delimiters do
      [delimiter] ->
        delimiter

      [] ->
        AshAsyncApi.Domain.Info.default_delimiter(domain) || "/"

      conflicting ->
        AshAsyncApi.Domain.Info.default_delimiter(domain) ||
          raise AshAsyncApi.Error.DelimiterConflict.exception(
                  channel: channel.name,
                  servers: channel_servers,
                  delimiters: conflicting,
                  domain: domain
                )
    end
  end

  # Channel names only need disambiguating when the same name covers more than one
  # address. In the overwhelmingly common case the key is just the name.
  defp assign_keys(scoped_channels) do
    counts =
      scoped_channels
      |> Enum.frequencies_by(fn {_scope, channel} -> channel.name end)

    Enum.map(scoped_channels, fn {scope, channel} ->
      key =
        if Map.get(counts, channel.name, 0) > 1 do
          disambiguate(scope, channel)
        else
          channel.name
        end

      {key, scope, channel}
    end)
  end

  defp disambiguate(%{resource: nil, domain: domain}, channel) do
    :"#{Macro.underscore(short_name(domain))}_#{channel.name}"
  end

  defp disambiguate(%{resource: resource}, channel) do
    type = AshAsyncApi.Resource.Info.type(resource) || Macro.underscore(short_name(resource))
    :"#{type}_#{channel.name}"
  end

  defp resolve_servers(%{servers: []}, domain, server_names) do
    case AshAsyncApi.Domain.Info.default_server(domain) do
      nil -> server_names
      default -> [default]
    end
  end

  defp resolve_servers(%{servers: servers}, _domain, _server_names), do: servers

  defp resolve_operations(declarations, channels) do
    Enum.flat_map(declarations, fn {scope, _channels, operations} ->
      Enum.map(operations, fn operation ->
        resource = operation.resource || scope.resource
        channel = find_channel(channels, operation.channel, scope, resource)

        %ResolvedOperation{
          name: operation.name,
          operation: operation,
          resource: resource,
          domain: scope.domain,
          action: operation.action,
          direction: operation.direction,
          channel_key: channel.key,
          address: channel.address,
          compiled_address: channel.compiled,
          servers: channel.servers,
          message_name: operation.message_name,
          content_type: content_type(operation, resource, scope.domain),
          reply_channel_key: reply_channel_key(operation, channels, scope, resource)
        }
      end)
    end)
  end

  defp find_channel(channels, name, scope, resource) do
    # Resource-scoped channels win over domain-scoped ones with the same name.
    Enum.find(channels, &(&1.name == name and &1.resource == resource)) ||
      Enum.find(
        channels,
        &(&1.name == name and is_nil(&1.resource) and &1.domain == scope.domain)
      ) ||
      Enum.find(channels, &(&1.name == name)) ||
      raise AshAsyncApi.Error.UnknownChannel.exception(
              channel: name,
              resource: resource,
              domain: scope.domain,
              known: Enum.map(channels, & &1.name)
            )
  end

  defp reply_channel_key(%{reply_channel: nil}, _channels, _scope, _resource), do: nil

  defp reply_channel_key(%{reply_channel: name}, channels, scope, resource) do
    find_channel(channels, name, scope, resource).key
  end

  defp content_type(operation, resource, domain) do
    operation.content_type ||
      (resource && AshAsyncApi.Resource.Info.default_content_type(resource)) ||
      AshAsyncApi.Domain.Info.default_content_type(domain)
  end

  defp index_outbound(operations) do
    Enum.group_by(operations, &{&1.resource, &1.action, &1.direction})
  end

  defp has_direction?(%ResolvedChannel{operations: operations}, direction) do
    Enum.any?(operations, &(&1.direction == direction))
  end

  defp short_name(module), do: module |> Module.split() |> List.last()
end
