defmodule AshAsyncApi.Resource.Info do
  @moduledoc "Introspection helpers for `AshAsyncApi.Resource`."

  alias Spark.Dsl.Extension

  @doc "The AsyncAPI type name for the resource."
  @spec type(Spark.Dsl.t() | Ash.Resource.t()) :: String.t() | nil
  def type(resource) do
    Extension.get_opt(resource, [:async_api], :type, nil, true)
  end

  @doc "The default content type for the resource's messages."
  @spec default_content_type(Spark.Dsl.t() | Ash.Resource.t()) :: String.t() | nil
  def default_content_type(resource) do
    Extension.get_opt(resource, [:async_api], :default_content_type, nil, true)
  end

  @doc "Whether `nil` payload values are published."
  @spec include_nil_values?(Spark.Dsl.t() | Ash.Resource.t()) :: boolean()
  def include_nil_values?(resource) do
    Extension.get_opt(resource, [:async_api], :include_nil_values?, false, true)
  end

  @doc "Fields hidden from every payload."
  @spec hide_fields(Spark.Dsl.t() | Ash.Resource.t()) :: [atom()]
  def hide_fields(resource) do
    Extension.get_opt(resource, [:async_api], :hide_fields, [], true)
  end

  @doc "The allow-list of payload fields, or `nil` when unset."
  @spec show_fields(Spark.Dsl.t() | Ash.Resource.t()) :: [atom()] | nil
  def show_fields(resource) do
    Extension.get_opt(resource, [:async_api], :show_fields, nil, true)
  end

  @doc "Whether JSON Schema payloads are derived from the resource."
  @spec derive_payload_schema?(Spark.Dsl.t() | Ash.Resource.t()) :: boolean()
  def derive_payload_schema?(resource) do
    Extension.get_opt(resource, [:async_api], :derive_payload_schema?, true, true)
  end

  @doc "Whether `publish` operations fire off Ash notifications."
  @spec publish_on_notification?(Spark.Dsl.t() | Ash.Resource.t()) :: boolean()
  def publish_on_notification?(resource) do
    Extension.get_opt(resource, [:async_api], :publish_on_notification?, true, true)
  end

  @doc "Tags applied to everything derived from this resource."
  @spec tags(Spark.Dsl.t() | Ash.Resource.t()) :: [String.t()]
  def tags(resource) do
    Extension.get_opt(resource, [:async_api], :tags, [], true)
  end

  @doc """
  Whether a field may appear in a payload, honouring `show_fields`/`hide_fields`.
  """
  @spec show_field?(Spark.Dsl.t() | Ash.Resource.t(), atom()) :: boolean()
  def show_field?(resource, field) do
    show = show_fields(resource)
    hide = hide_fields(resource)

    field not in hide and (is_nil(show) or field in show)
  end

  @doc "The channels declared on the resource."
  @spec channels(Spark.Dsl.t() | Ash.Resource.t()) :: [AshAsyncApi.Channel.t()]
  def channels(resource) do
    Extension.get_entities(resource, [:async_api, :channels])
  end

  @doc """
  The channels available to the resource — its own, plus those declared on the
  given domain(s).

  Resource-level channels take precedence over domain-level ones with the same name.
  """
  @spec channels(Spark.Dsl.t() | Ash.Resource.t(), module() | [module()]) ::
          [AshAsyncApi.Channel.t()]
  def channels(resource, domain_or_domains) do
    own = channels(resource)
    own_names = MapSet.new(own, & &1.name)

    inherited =
      domain_or_domains
      |> List.wrap()
      |> Enum.flat_map(&AshAsyncApi.Domain.Info.channels/1)
      |> Enum.reject(&MapSet.member?(own_names, &1.name))

    own ++ inherited
  end

  @doc "Look up a channel by name, including channels inherited from `domains`."
  @spec channel(Spark.Dsl.t() | Ash.Resource.t(), atom(), module() | [module()]) ::
          AshAsyncApi.Channel.t() | nil
  def channel(resource, name, domains \\ []) do
    resource
    |> channels(domains)
    |> Enum.find(&(&1.name == name))
  end

  @doc "The operations declared on the resource."
  @spec operations(Spark.Dsl.t() | Ash.Resource.t()) :: [AshAsyncApi.Operation.t()]
  def operations(resource) do
    Extension.get_entities(resource, [:async_api, :operations])
  end

  @doc """
  The operations for this resource — its own, plus those declared for it on the
  given domain(s).
  """
  @spec operations(Spark.Dsl.t() | Ash.Resource.t(), module() | [module()]) ::
          [AshAsyncApi.Operation.t()]
  def operations(resource, domain_or_domains) do
    module =
      if is_atom(resource) do
        resource
      else
        Extension.get_persisted(resource, :module)
      end

    domain_or_domains
    |> List.wrap()
    |> Enum.flat_map(&AshAsyncApi.Domain.Info.operations/1)
    |> Enum.filter(&(&1.resource == module))
    |> Enum.concat(operations(resource))
  end

  @doc "The operations of a given direction (`:send` or `:receive`)."
  @spec operations(Spark.Dsl.t() | Ash.Resource.t(), module() | [module()], :send | :receive) ::
          [AshAsyncApi.Operation.t()]
  def operations(resource, domains, direction) do
    resource
    |> operations(domains)
    |> Enum.filter(&(&1.direction == direction))
  end

  @doc """
  Whether the resource has the `AshAsyncApi.Resource` extension.
  """
  @spec async_api?(module()) :: boolean()
  def async_api?(resource) when is_atom(resource) do
    Spark.Dsl.is?(resource, Ash.Resource) and
      AshAsyncApi.Resource in Spark.extensions(resource)
  end

  def async_api?(_), do: false
end
