defmodule Helpdesk.AsyncApiRouter do
  @moduledoc """
  The router. Owns the `Group` instance that fans messages out across the cluster, and
  supervises the MQTT and NATS connections.
  """

  use AshAsyncApi.Router, domains: [Helpdesk.Support]
end
