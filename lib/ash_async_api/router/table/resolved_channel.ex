defmodule AshAsyncApi.Router.Table.ResolvedChannel do
  @moduledoc """
  A channel with everything the runtime needs already worked out: a unique document key, the
  delimiter resolved from its servers, the address compiled with it, the servers it is
  reachable on, and the operations that use it.

  `address` is the rendered `{braced}` template — `"helpdesk/tickets/{id}/events"` — which is
  what goes into the AsyncAPI document. The DSL holds only the segment list; the delimiter is
  not known until the channel is matched with its servers, which is why this happens here
  rather than at compile time.
  """

  defstruct [
    :key,
    :name,
    :address,
    :compiled,
    :delimiter,
    :channel,
    :domain,
    :resource,
    servers: [],
    operations: []
  ]

  @type t :: %__MODULE__{
          key: atom(),
          name: atom(),
          address: String.t() | nil,
          compiled: AshAsyncApi.Address.t() | nil,
          delimiter: String.t() | nil,
          channel: AshAsyncApi.Channel.t(),
          domain: module(),
          resource: module() | nil,
          servers: [atom()],
          operations: [AshAsyncApi.Router.Table.ResolvedOperation.t()]
        }

  @doc "The operations on this channel with the given direction."
  @spec operations(t(), :send | :receive) :: [AshAsyncApi.Router.Table.ResolvedOperation.t()]
  def operations(%__MODULE__{operations: operations}, direction) do
    Enum.filter(operations, &(&1.direction == direction))
  end

  @doc "The declared parameter for a name, if the channel described one."
  @spec parameter(t(), atom()) :: AshAsyncApi.Channel.Parameter.t() | nil
  def parameter(%__MODULE__{channel: channel}, name) do
    Enum.find(channel.parameters, &(&1.name == name))
  end
end
