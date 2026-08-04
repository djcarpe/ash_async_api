defmodule AshAsyncApi.Test.Crm.Lead do
  @moduledoc """
  A resource covered by the `AshAsyncApi.Test.Crm.Events` fragment, with a
  custom-named create (`:import`) that only `publish_all` can cover, and an explicit
  `publish` (`:qualify`) that takes one action out of `publish_all`'s hands.
  """

  use Ash.Resource,
    domain: AshAsyncApi.Test.Crm,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource],
    fragments: [AshAsyncApi.Test.Crm.Events]

  ets do
    private? true
  end

  async_api do
    operations do
      publish :qualify, :events do
        event_name "qualified"
      end
    end
  end

  attributes do
    uuid_primary_key :id

    attribute :name, :string, public?: true

    attribute :status, :atom do
      public? true
      default :new
      constraints one_of: [:new, :qualified]
    end
  end

  actions do
    defaults [:read, :destroy, create: :*, update: :*]

    create :import do
      accept [:name]
    end

    update :qualify do
      accept []
      change set_attribute(:status, :qualified)
    end
  end
end
