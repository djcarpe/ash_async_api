defmodule AshAsyncApi.Test.Helpdesk.Comment do
  @moduledoc """
  A comment. Its channels and operations are declared on the domain rather than here, which
  exercises the domain-level DSL — and its addresses are built from relationship paths, so a
  comment's address carries the ticket and organization it belongs to.
  """

  use Ash.Resource,
    domain: AshAsyncApi.Test.Helpdesk,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource]

  ets do
    private? true
  end

  async_api do
    type "comment"
  end

  attributes do
    uuid_primary_key :id

    attribute :author, :string, public?: true, allow_nil?: false

    attribute :body, :string do
      allow_nil? false
      public? true
    end

    create_timestamp :created_at, public?: true
  end

  relationships do
    belongs_to :ticket, AshAsyncApi.Test.Helpdesk.Ticket do
      allow_nil? false
      public? true
      attribute_writable? true
    end
  end

  actions do
    defaults [:read, :destroy]

    create :add do
      primary? true
      description "Add a comment to a ticket"
      accept [:ticket_id, :author, :body]
    end
  end
end
