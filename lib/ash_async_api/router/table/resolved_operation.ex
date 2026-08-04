defmodule AshAsyncApi.Router.Table.ResolvedOperation do
  @moduledoc """
  An operation with its channel, content type and reply target resolved.

  The address and server list are copied down from the channel so the publish path
  needs one lookup instead of two.
  """

  defstruct [
    :name,
    :operation,
    :resource,
    :domain,
    :action,
    :direction,
    :channel_key,
    :address,
    :compiled_address,
    :message_name,
    :content_type,
    :reply_channel_key,
    :event_verb,
    servers: []
  ]

  @type t :: %__MODULE__{
          name: atom(),
          operation: AshAsyncApi.Operation.t(),
          resource: module() | nil,
          domain: module(),
          action: atom(),
          direction: :send | :receive,
          channel_key: atom(),
          address: String.t() | nil,
          compiled_address: AshAsyncApi.Address.t() | nil,
          message_name: String.t(),
          content_type: String.t(),
          reply_channel_key: atom() | nil,
          event_verb: String.t() | nil,
          servers: [atom()]
        }
end
