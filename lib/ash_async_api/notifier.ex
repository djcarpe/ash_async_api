defmodule AshAsyncApi.Notifier do
  @moduledoc """
  Publishes messages off Ash notifications.

  Attached automatically by
  `AshAsyncApi.Resource.Transformers.SetupNotifier` to any resource with `publish`
  operations on create/update/destroy actions, which is what makes publishing
  automatic — the resource author declares an operation and the messages start
  flowing.

  ## Which router?

  A resource does not know which router serves it, and there may be more than one. So
  the notifier publishes through every router configured for the resource's OTP
  application:

      config :helpdesk, ash_async_api_routers: [Helpdesk.AsyncApiRouter]

  When that is not set, the notifier finds routers by looking for modules that serve
  the resource's domain. That works, but it scans the code path on first use, so
  configuring it explicitly is worth doing in production.

  ## Transactions

  Ash holds notifications until the enclosing transaction commits, so a message is
  never published for a change that gets rolled back.
  """

  use Ash.Notifier

  require Logger

  @impl true
  def notify(%Ash.Notifier.Notification{} = notification) do
    for router <- routers(notification.resource) do
      case AshAsyncApi.Publisher.publish_notification(router, notification) do
        {:ok, _envelopes} ->
          :ok

        {:error, reason} ->
          Logger.error("""
          AshAsyncApi failed to publish for \
          #{inspect(notification.resource)}.#{notification.action.name}: \
          #{format(reason)}
          """)
      end
    end

    :ok
  end

  @doc """
  The routers that serve a resource.
  """
  @spec routers(module()) :: [module()]
  def routers(resource) do
    otp_app = otp_app(resource)

    case configured_routers(otp_app) do
      [] -> discovered_routers(resource, otp_app)
      routers -> routers
    end
  end

  defp configured_routers(nil), do: []

  defp configured_routers(otp_app) do
    Application.get_env(otp_app, :ash_async_api_routers, []) |> List.wrap()
  end

  # Cached in `:persistent_term` because scanning modules is expensive and the answer
  # cannot change without a recompile.
  defp discovered_routers(resource, otp_app) do
    key = {__MODULE__, :discovered, resource}

    case :persistent_term.get(key, nil) do
      nil ->
        routers = discover(resource, otp_app)
        :persistent_term.put(key, routers)
        warn_if_empty(resource, routers)
        routers

      routers ->
        routers
    end
  end

  defp discover(resource, otp_app) do
    domain = Ash.Resource.Info.domain(resource)

    otp_app
    |> modules()
    |> Enum.filter(fn module ->
      Code.ensure_loaded?(module) and function_exported?(module, :__ash_async_api_config__, 0) and
        domain in module.__ash_async_api_config__().domains
    end)
  end

  defp modules(nil), do: []

  defp modules(otp_app) do
    case :application.get_key(otp_app, :modules) do
      {:ok, modules} -> modules
      _ -> []
    end
  end

  defp otp_app(resource) do
    Ash.Resource.Info.domain(resource)
    |> case do
      nil -> nil
      domain -> Application.get_application(domain)
    end || Application.get_application(resource)
  end

  defp warn_if_empty(resource, []) do
    Logger.warning("""
    #{inspect(resource)} has AshAsyncApi publish operations, but no router was found \
    to publish them through, so its messages are going nowhere.

    Configure the routers explicitly:

        config #{inspect(otp_app(resource) || :my_app)}, ash_async_api_routers: [MyApp.AsyncApiRouter]
    """)
  end

  defp warn_if_empty(_resource, _routers), do: :ok

  defp format(%{__exception__: true} = error), do: Exception.message(error)
  defp format(error), do: inspect(error)
end
