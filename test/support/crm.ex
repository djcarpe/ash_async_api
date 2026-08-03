defmodule AshAsyncApi.Test.Crm.Headers do
  @moduledoc "A captured-function header builder, the shape a real service uses."

  def build(_record) do
    %{"service" => "crm"}
  end
end

defmodule AshAsyncApi.Test.Crm.Events do
  @moduledoc """
  A shared fragment publishing every create/update/destroy action to one
  special-segment channel — the CRUD-event-firehose shape. Applied to every resource
  in the `AshAsyncApi.Test.Crm` domain, and resolved into a distinct channel per
  resource when the routing table is built.
  """

  use Spark.Dsl.Fragment, of: Ash.Resource, extensions: [AshAsyncApi.Resource]

  async_api do
    channels do
      channel :events, [:_domain, :_resource, :_event, :_pkey]
    end

    operations do
      publish_all :create, :events do
        headers &AshAsyncApi.Test.Crm.Headers.build/1
      end

      publish_all :update, :events
      publish_all :destroy, :events
    end
  end
end

defmodule AshAsyncApi.Test.Crm do
  @moduledoc "A domain whose resources publish CRUD events through a shared fragment."

  use Ash.Domain, extensions: [AshAsyncApi.Domain]

  async_api do
    type "crm"

    info do
      title "CRM Events"
    end

    servers do
      server :bus, "erlang-distribution" do
        protocol :erlang
        # NATS-style dotted addresses, without needing a broker in the tests.
        delimiter "."
        transport AshAsyncApi.Transport.Local
      end
    end
  end

  resources do
    resource AshAsyncApi.Test.Crm.Lead
    resource AshAsyncApi.Test.Crm.Tag
  end
end
