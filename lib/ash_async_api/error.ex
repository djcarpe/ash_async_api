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
    @moduledoc "A templated address could not be filled in."
    defexception [:address, :missing, :channel, :subject]

    @impl true
    def message(%{address: address, missing: missing, channel: channel}) do
      """
      Cannot build an address for channel #{inspect(channel)}.

          address: #{inspect(address)}
          missing values for: #{inspect(missing)}

      Each `{parameter}` in the address needs a value. By default the value is read \
      from the field of the same name on the record being published. If the field has \
      a different name, point at it:

          channel #{inspect(channel)}, #{inspect(address)} do
            parameter #{inspect(hd(missing))}, source: :the_actual_field
          end
      """
    end
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
