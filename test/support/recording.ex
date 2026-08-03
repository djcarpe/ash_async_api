defmodule AshAsyncApi.Test.RecordingTransport do
  @moduledoc """
  A transport that records what happens to it: the context it was started with and
  every publish it received. This is how the runtime server configuration is observed
  — a disabled server never starts the agent, and merged `transport_opts` show up in
  the recorded context.
  """

  use AshAsyncApi.Transport

  @impl true
  def wildcard_style, do: {:single, "*"}

  @impl true
  def child_spec(context) do
    %{
      id: {__MODULE__, context.server.name},
      start:
        {Agent, :start_link,
         [
           fn -> %{context: context, publishes: []} end,
           [name: name(context.server.name)]
         ]}
    }
  end

  @impl true
  def publish(context, address, body, opts) do
    case Process.whereis(name(context.server.name)) do
      nil ->
        {:error, :not_started}

      _pid ->
        Agent.update(name(context.server.name), fn state ->
          %{state | publishes: [{address, IO.iodata_to_binary(body), opts} | state.publishes]}
        end)
    end
  end

  @impl true
  def subscribe(_context, _filter), do: :ok

  def started?(server_name), do: is_pid(Process.whereis(name(server_name)))

  def state(server_name), do: Agent.get(name(server_name), & &1)

  defp name(server_name), do: :"ash_async_api_recording_#{server_name}"
end

defmodule AshAsyncApi.Test.Rec.Widget do
  @moduledoc "A resource publishing through the recording transport."

  use Ash.Resource,
    domain: AshAsyncApi.Test.Rec,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource]

  ets do
    private? true
  end

  async_api do
    channels do
      channel :events, [:_domain, :_resource, :_event, :_pkey]
    end

    operations do
      publish_all :create, :events
    end
  end

  attributes do
    uuid_primary_key :id

    attribute :label, :string, public?: true
  end

  actions do
    defaults [:read, create: :*]
  end
end

defmodule AshAsyncApi.Test.Rec do
  @moduledoc "A domain with one recording-transport server, for runtime config tests."

  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    type "rec"

    servers do
      server :rec, "recorder.example.com:4222" do
        protocol :nats
        transport AshAsyncApi.Test.RecordingTransport
        transport_opts compile_time: true, queue_group: "compile"
      end
    end
  end

  resources do
    resource AshAsyncApi.Test.Rec.Widget
  end
end

defmodule AshAsyncApi.Test.RecRouter do
  @moduledoc false

  use AshAsyncApi.Router, domains: [AshAsyncApi.Test.Rec]
end
