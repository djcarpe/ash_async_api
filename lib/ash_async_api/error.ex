defmodule AshAsyncApi.Error do
  @moduledoc """
  The exceptions AshAsyncApi raises.

  These are deliberately specific: a message that fails to route should say which
  address did not match and what the router does know about, because that is the
  information you need at 3am.
  """

  defmodule UnknownChannel do
    @moduledoc "An operation references a channel that nothing declares."
    defexception [:channel, :resource, :domain, :known]

    @impl true
    def message(%{channel: channel, resource: resource, domain: domain, known: known}) do
      """
      Unknown channel #{inspect(channel)}.

      Referenced by an operation on #{describe(resource, domain)}, but no channel by \
      that name is declared on the resource or on the domain.

      Known channels: #{inspect(Enum.sort(known))}
      """
    end

    defp describe(nil, domain), do: inspect(domain)
    defp describe(resource, domain), do: "#{inspect(resource)} (domain #{inspect(domain)})"
  end

  defmodule DelimiterConflict do
    @moduledoc "A channel spans servers whose address delimiters disagree."
    defexception [:channel, :servers, :delimiters, :domain]

    @impl true
    def message(%{channel: channel, servers: servers, delimiters: delimiters, domain: domain}) do
      """
      Channel #{inspect(channel)} is on servers #{inspect(servers)}, which join address \
      segments differently: #{inspect(delimiters)}.

      A channel has one address, so AshAsyncApi cannot pick for you. Either put the channel \
      on servers that agree, or say what it should be:

          channel #{inspect(channel)}, [...] do
            delimiter #{inspect(hd(delimiters))}
          end

      To settle it for every channel in the domain instead:

          async_api do
            default_delimiter #{inspect(hd(delimiters))}
          end

      (in #{inspect(domain)})
      """
    end
  end

  defmodule UnknownOperation do
    @moduledoc "A publish was requested for an action with no matching operation."
    defexception [:resource, :action, :direction, :router, :known]

    @impl true
    def message(%{resource: resource, action: action, direction: direction, known: known}) do
      """
      No #{direction_word(direction)} operation for #{inspect(resource)}.#{action}.

      Declare one on the resource:

          async_api do
            operations do
              #{direction_word(direction)} #{inspect(action)}, :some_channel
            end
          end

      Operations currently declared for this resource: #{inspect(known)}
      """
    end

    defp direction_word(:send), do: "publish"
    defp direction_word(:receive), do: "subscribe"
  end

  defmodule NoRoute do
    @moduledoc "An inbound message's address matched no channel."
    defexception [:address, :server, :router, :known]

    @impl true
    def message(%{address: address, server: server, known: known}) do
      """
      No channel matches the address #{inspect(address)} on server #{inspect(server)}.

      Addresses this router receives on:
      #{Enum.map_join(known, "\n", &"  #{&1}")}
      """
    end
  end

  defmodule MissingAddressParams do
    @moduledoc "An address could not be filled in from the record."
    defexception [:address, :missing, :channel, :subject, :paths]

    @impl true
    def message(%{address: address, missing: missing, channel: channel} = error) do
      """
      Cannot build an address for channel #{inspect(channel)}.

          address: #{inspect(address)}
          missing values for: #{inspect(missing)}
      #{sources(error, missing)}
      Every non-literal segment needs a value from the record. A missing one usually means \
      the field is nil, or that a relationship in the path could not be loaded.

      If the value lives somewhere else, address it directly:

          channel #{inspect(channel)}, ["...", #{inspect(suggest(error, missing))}]
      """
    end

    defp sources(%{paths: paths}, missing) when is_map(paths) do
      lines =
        Enum.map_join(missing, "\n", fn name ->
          "        #{name} <- #{inspect(Map.get(paths, name) || [name])}"
        end)

      "\n      read from:\n#{lines}\n"
    end

    defp sources(_error, _missing), do: ""

    defp suggest(%{paths: paths}, [first | _]) when is_map(paths) do
      Map.get(paths, first) || first
    end

    defp suggest(_error, [first | _]), do: first
  end

  defmodule NoTransport do
    @moduledoc "Publishing was attempted on a channel whose servers have no transport."
    defexception [:channel, :servers, :domain]

    @impl true
    def message(%{channel: channel, servers: servers, domain: domain}) do
      """
      Channel #{inspect(channel)} has no server with a transport, so there is nowhere \
      to publish to.

      Servers on the channel: #{inspect(servers)}

      Give one of them a transport:

          async_api do
            servers do
              server :broker, "localhost:1883" do
                protocol :mqtt
                transport AshAsyncApi.Transport.Mqtt
              end
            end
          end

      (in #{inspect(domain)})
      """
    end
  end

  defmodule InvalidPayload do
    @moduledoc "An inbound message's payload could not be turned into action input."
    defexception [:reason, :payload, :operation, :address]

    @impl true
    def message(%{reason: reason, operation: operation, address: address}) do
      """
      Could not build input for operation #{inspect(operation)} from the message \
      received at #{inspect(address)}.

      #{format(reason)}
      """
    end

    defp format(reason) when is_binary(reason), do: reason
    defp format(%{__exception__: true} = reason), do: Exception.message(reason)
    defp format(reason), do: inspect(reason)
  end
end
