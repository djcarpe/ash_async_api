defmodule AshAsyncApi.Domain.Info do
  @moduledoc "Introspection helpers for `AshAsyncApi.Domain`."

  alias Spark.Dsl.Extension

  @doc "The application id, rendered as the AsyncAPI document's `id`."
  @spec id(Spark.Dsl.t() | Ash.Domain.t()) :: String.t() | nil
  def id(domain) do
    Extension.get_opt(domain, [:async_api], :id, nil, true)
  end

  @doc "The content type used by messages that do not specify their own."
  @spec default_content_type(Spark.Dsl.t() | Ash.Domain.t()) :: String.t()
  def default_content_type(domain) do
    Extension.get_opt(domain, [:async_api], :default_content_type, "application/json", true)
  end

  @doc """
  The name the `:_domain` address segment resolves to. Defaults to the domain's short
  module name in snake case, so `Helpdesk.Support` is `"support"`.
  """
  @spec type(Spark.Dsl.t() | Ash.Domain.t()) :: String.t()
  def type(domain) do
    Extension.get_opt(domain, [:async_api], :type, nil, true) || default_type(domain)
  end

  defp default_type(domain) do
    module =
      if is_atom(domain) do
        domain
      else
        Extension.get_persisted(domain, :module)
      end

    module |> Module.split() |> List.last() |> Macro.underscore()
  end

  @doc """
  How the `:_domain` and `:_resource` address segments render for this domain's
  channels: `:snake` (the default), `:camel`, or a one-argument function of the
  domain or resource module.
  """
  @spec segment_naming(Spark.Dsl.t() | Ash.Domain.t()) ::
          :snake | :camel | (module() -> String.t())
  def segment_naming(domain) do
    Extension.get_opt(domain, [:async_api], :segment_naming, :snake, true)
  end

  @doc "The server used by channels that name none."
  @spec default_server(Spark.Dsl.t() | Ash.Domain.t()) :: atom() | nil
  def default_server(domain) do
    Extension.get_opt(domain, [:async_api], :default_server, nil, true)
  end

  @doc """
  The domain-wide address delimiter override, or `nil` to let each channel's servers decide.
  """
  @spec default_delimiter(Spark.Dsl.t() | Ash.Domain.t()) :: String.t() | nil
  def default_delimiter(domain) do
    Extension.get_opt(domain, [:async_api], :default_delimiter, nil, true)
  end

  @doc "Reusable security schemes for `components.securitySchemes`."
  @spec security_schemes(Spark.Dsl.t() | Ash.Domain.t()) :: map()
  def security_schemes(domain) do
    Extension.get_opt(domain, [:async_api], :security_schemes, %{}, true)
  end

  @doc "Whether telemetry spans are emitted."
  @spec trace?(Spark.Dsl.t() | Ash.Domain.t()) :: boolean()
  def trace?(domain) do
    Extension.get_opt(domain, [:async_api], :trace?, true, true)
  end

  @doc "The servers declared on the domain."
  @spec servers(Spark.Dsl.t() | Ash.Domain.t()) :: [AshAsyncApi.Server.t()]
  def servers(domain) do
    Extension.get_entities(domain, [:async_api, :servers]) || []
  end

  @doc "Look up a server by name."
  @spec server(Spark.Dsl.t() | Ash.Domain.t(), atom()) :: AshAsyncApi.Server.t() | nil
  def server(domain, name) do
    domain |> servers() |> Enum.find(&(&1.name == name))
  end

  @doc "The servers that have a transport, i.e the ones that actually get started."
  @spec connected_servers(Spark.Dsl.t() | Ash.Domain.t()) :: [AshAsyncApi.Server.t()]
  def connected_servers(domain) do
    domain |> servers() |> Enum.reject(&is_nil(&1.transport))
  end

  @doc "The channels declared on the domain."
  @spec channels(Spark.Dsl.t() | Ash.Domain.t()) :: [AshAsyncApi.Channel.t()]
  def channels(domain) do
    Extension.get_entities(domain, [:async_api, :channels]) || []
  end

  @doc "Look up a domain channel by name."
  @spec channel(Spark.Dsl.t() | Ash.Domain.t(), atom()) :: AshAsyncApi.Channel.t() | nil
  def channel(domain, name) do
    domain |> channels() |> Enum.find(&(&1.name == name))
  end

  @doc "The operations declared on the domain."
  @spec operations(Spark.Dsl.t() | Ash.Domain.t()) :: [AshAsyncApi.Operation.t()]
  def operations(domain) do
    Extension.get_entities(domain, [:async_api, :operations]) || []
  end

  @doc "The `info` block, as a map ready for rendering."
  @spec info(Spark.Dsl.t() | Ash.Domain.t()) :: map()
  def info(domain) do
    %{
      title:
        Extension.get_opt(domain, [:async_api, :info], :title, nil, true) || default_title(domain),
      version: Extension.get_opt(domain, [:async_api, :info], :version, "1.0.0", true),
      description: Extension.get_opt(domain, [:async_api, :info], :description, nil, true),
      terms_of_service:
        Extension.get_opt(domain, [:async_api, :info], :terms_of_service, nil, true),
      contact_name: Extension.get_opt(domain, [:async_api, :info], :contact_name, nil, true),
      contact_url: Extension.get_opt(domain, [:async_api, :info], :contact_url, nil, true),
      contact_email: Extension.get_opt(domain, [:async_api, :info], :contact_email, nil, true),
      license_name: Extension.get_opt(domain, [:async_api, :info], :license_name, nil, true),
      license_url: Extension.get_opt(domain, [:async_api, :info], :license_url, nil, true),
      external_docs: Extension.get_opt(domain, [:async_api, :info], :external_docs, nil, true),
      tags: Extension.get_opt(domain, [:async_api, :info], :tags, [], true)
    }
  end

  @doc """
  Whether the domain has the `AshAsyncApi.Domain` extension.
  """
  @spec async_api?(module()) :: boolean()
  def async_api?(domain) when is_atom(domain) do
    Spark.Dsl.is?(domain, Ash.Domain) and AshAsyncApi.Domain in Spark.extensions(domain)
  end

  def async_api?(_), do: false

  defp default_title(domain) do
    module =
      if is_atom(domain) do
        domain
      else
        Extension.get_persisted(domain, :module)
      end

    module
    |> Module.split()
    |> List.last()
    |> then(&Regex.replace(~r/([a-z\d])([A-Z])/, &1, "\\1 \\2"))
  end
end
