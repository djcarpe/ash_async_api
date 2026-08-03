defmodule AshAsyncApi.Test.Router do
  @moduledoc """
  The default test router. `ignore_own_messages?` is left at its default of `true`, so
  this router's own publishes do not trigger its `subscribe` operations.
  """

  use AshAsyncApi.Router, domains: [AshAsyncApi.Test.Helpdesk]
end

defmodule AshAsyncApi.Test.CrmRouter do
  @moduledoc """
  The router for the Crm domain, whose resources publish through a shared
  special-segment fragment.
  """

  use AshAsyncApi.Router, domains: [AshAsyncApi.Test.Crm]
end

defmodule AshAsyncApi.Test.LoopbackRouter do
  @moduledoc """
  A router with `ignore_own_messages?: false`, so that publishing a command through
  `AshAsyncApi.Transport.Local` loops back and runs the `subscribe` operation. This is
  how the receive path is exercised without a broker.
  """

  use AshAsyncApi.Router,
    domains: [AshAsyncApi.Test.Helpdesk],
    ignore_own_messages?: false
end
