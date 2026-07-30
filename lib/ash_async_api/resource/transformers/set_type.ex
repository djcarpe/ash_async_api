defmodule AshAsyncApi.Resource.Transformers.SetType do
  @moduledoc """
  Defaults `async_api.type` to the resource's short name.

  So `Helpdesk.Support.Ticket` becomes `"ticket"` without having to say so.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    case Transformer.get_option(dsl, [:async_api], :type) do
      nil ->
        {:ok, Transformer.set_option(dsl, [:async_api], :type, default_type(dsl))}

      _type ->
        {:ok, dsl}
    end
  end

  @impl true
  def before?(AshAsyncApi.Resource.Transformers.DefaultOperationNames), do: true
  def before?(_), do: false

  defp default_type(dsl) do
    dsl
    |> Ash.Resource.Info.short_name()
    |> to_string()
  end
end
