import Config

if Mix.env() == :test do
  config :ash_async_api, ash_domains: [AshAsyncApi.Test.Helpdesk]

  # The notifier publishes through every configured router. Only the default one is
  # listed: `AshAsyncApi.Test.LoopbackRouter` exists to exercise the inbound path and is
  # started explicitly by the tests that need it.
  config :ash_async_api, :ash_async_api_routers, [AshAsyncApi.Test.Router]

  config :logger, level: :warning
end
