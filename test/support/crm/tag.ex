defmodule AshAsyncApi.Test.Crm.Tag do
  @moduledoc """
  A composite-primary-key resource covered by the `AshAsyncApi.Test.Crm.Events`
  fragment: `:_pkey` has to pack both key fields into one address token.
  """

  use Ash.Resource,
    domain: AshAsyncApi.Test.Crm,
    data_layer: Ash.DataLayer.Ets,
    extensions: [AshAsyncApi.Resource],
    fragments: [AshAsyncApi.Test.Crm.Events]

  ets do
    private? true
  end

  attributes do
    attribute :namespace, :string do
      primary_key? true
      allow_nil? false
      public? true
    end

    attribute :name, :string do
      primary_key? true
      allow_nil? false
      public? true
    end
  end

  actions do
    defaults [:read, create: :*]
  end
end
