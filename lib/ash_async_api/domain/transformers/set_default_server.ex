defmodule AshAsyncApi.Domain.Transformers.SetDefaultServer do
  @moduledoc """
  When exactly one server is declared and no `default_server` is set, make it the
  default.

  Single-broker applications are the common case, and having to repeat
  `servers [:mqtt]` on every channel is noise.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    case {Transformer.get_option(dsl, [:async_api], :default_server),
          Transformer.get_entities(dsl, [:async_api, :servers])} do
      {nil, [%{name: name}]} ->
        {:ok, Transformer.set_option(dsl, [:async_api], :default_server, name)}

      _ ->
        {:ok, dsl}
    end
  end
end
