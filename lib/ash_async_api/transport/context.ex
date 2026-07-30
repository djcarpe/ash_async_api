defmodule AshAsyncApi.Transport.Context do
  @moduledoc """
  Everything a transport needs to know about where it sits.

  Built by `AshAsyncApi.Supervisor` when it starts a server's transport, and passed
  to every `AshAsyncApi.Transport` callback. Holding the router and domain here is
  what lets a transport call `AshAsyncApi.Transport.deliver/4` without the connection
  process having to remember any of it.
  """

  defstruct [
    :router,
    :domain,
    :server,
    :transport,
    :group,
    opts: [],
    private: %{}
  ]

  @type t :: %__MODULE__{
          router: module(),
          domain: module(),
          server: AshAsyncApi.Server.t(),
          transport: module(),
          group: atom(),
          opts: keyword(),
          private: map()
        }

  @doc """
  Build a context for a server on a router.
  """
  @spec new(module(), module(), AshAsyncApi.Server.t(), keyword()) :: t()
  def new(router, domain, %AshAsyncApi.Server{} = server, extra_opts \\ []) do
    %__MODULE__{
      router: router,
      domain: domain,
      server: server,
      transport: server.transport,
      group: AshAsyncApi.PubSub.group_name(router),
      opts: Keyword.merge(server.transport_opts, extra_opts)
    }
  end

  @doc """
  A name unique to this router/server pair, for registering the connection process.
  """
  @spec process_name(t()) :: atom()
  def process_name(%__MODULE__{router: router, server: server}) do
    Module.concat([router, "Transport", Macro.camelize(to_string(server.name))])
  end

  @doc """
  Fetch a transport option.
  """
  @spec opt(t(), atom(), term()) :: term()
  def opt(%__MODULE__{opts: opts}, key, default \\ nil), do: Keyword.get(opts, key, default)

  @doc """
  Fetch a required transport option, raising a message that says where to set it.
  """
  @spec opt!(t(), atom()) :: term()
  def opt!(%__MODULE__{opts: opts, server: server, transport: transport}, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        raise ArgumentError, """
        #{inspect(transport)} requires the #{inspect(key)} option.

        Set it in `transport_opts` on the server:

            server #{inspect(server.name)}, #{inspect(server.host)} do
              transport #{inspect(transport)}
              transport_opts [#{key}: ...]
            end
        """
    end
  end
end
