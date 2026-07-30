defmodule AshAsyncApi.Resource.Transformers.SetupNotifier do
  @moduledoc """
  Attaches `AshAsyncApi.Notifier` to every resource with the extension.

  This is what makes publishing automatic: create/update/destroy actions already
  emit `Ash.Notifier.Notification`s, so the notifier turns those into messages
  without the resource author wiring anything up.

  The notifier is attached whether or not the resource declares `publish` operations
  itself, because a resource's operations can equally be declared on its domain — and a
  resource cannot see that. Attaching unconditionally is the only way domain-declared
  publishing can work, and the cost is small: `AshAsyncApi.Notifier` looks the action up
  in the routing table and returns immediately when nothing matches.

  Setting `publish_on_notification? false` opts out entirely, for resources that
  publish only via explicit `AshAsyncApi.publish/3` calls.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    if needs_notifier?(dsl) do
      {:ok, add_notifier(dsl)}
    else
      {:ok, dsl}
    end
  end

  @impl true
  def after?(AshAsyncApi.Resource.Transformers.DefaultOperationNames), do: true
  def after?(_), do: false

  defp needs_notifier?(dsl) do
    Transformer.get_option(dsl, [:async_api], :publish_on_notification?, true)
  end

  defp add_notifier(dsl) do
    notifiers = Transformer.get_persisted(dsl, :simple_notifiers, [])

    if AshAsyncApi.Notifier in notifiers do
      dsl
    else
      Transformer.persist(dsl, :simple_notifiers, [AshAsyncApi.Notifier | notifiers])
    end
  end
end
