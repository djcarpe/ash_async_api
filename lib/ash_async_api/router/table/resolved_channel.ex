defmodule AshAsyncApi.Router.Table.ResolvedChannel do
  @moduledoc """
  A channel with everything the runtime needs already worked out: a unique document
  key, a compiled address, the servers it is reachable on, and the operations that
  use it.
  """

  defstruct [
    :key,
    :name,
    :address,
    :compiled,
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
