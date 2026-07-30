defmodule Helpdesk.MixProject do
  use Mix.Project

  def project do
    [
      app: :helpdesk,
      version: "0.1.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Helpdesk.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ash_async_api, path: "../.."},
      {:ash, "~> 3.0"},

      # The broker clients. AshAsyncApi does not depend on these — you add the one
      # you need, which is why this example lists both.
      {:emqtt, github: "emqx/emqtt", tag: "1.14.4", system_env: [{"BUILD_WITHOUT_QUIC", "1"}]},
      {:gnat, "~> 1.9"},

      # `ymlr` is an *optional* dependency of ash_async_api, so a consuming app that wants
      # `AshAsyncApi.Spec.to_yaml/2` has to ask for it explicitly.
      {:ymlr, "~> 5.0"},

      # Just to serve the generated AsyncAPI document over HTTP.
      {:bandit, "~> 1.5"},
      {:plug, "~> 1.16"}
    ]
  end
end
