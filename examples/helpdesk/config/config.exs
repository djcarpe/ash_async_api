import Config

config :helpdesk, ash_domains: [Helpdesk.Support]

# The notifier publishes through every router listed here. Configure it explicitly —
# otherwise AshAsyncApi has to scan the code path to find your routers.
config :helpdesk, ash_async_api_routers: [Helpdesk.AsyncApiRouter]

config :logger, :default_formatter,
  format: "$time [$level] $message\n",
  metadata: []

config :logger, level: :info
