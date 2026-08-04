import Config

if Mix.env() == :test do
  config :ash_async_api,
    ash_domains: [AshAsyncApi.Test.Helpdesk, AshAsyncApi.Test.Crm, AshAsyncApi.Test.Rec]

  # The notifier publishes through every configured router. `AshAsyncApi.Test.LoopbackRouter`
  # is not listed: it exists to exercise the inbound path and is started explicitly by the
  # tests that need it.
  config :ash_async_api, :ash_async_api_routers, [
    AshAsyncApi.Test.Router,
    AshAsyncApi.Test.CrmRouter,
    AshAsyncApi.Test.RecRouter
  ]

  config :logger, level: :warning
end
