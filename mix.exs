defmodule AshAsyncApi.MixProject do
  use Mix.Project

  @version "0.1.0"
  @description "The extension for building AsyncAPI 3.0 compliant event-driven APIs with Ash."

  def project do
    [
      app: :ash_async_api,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      description: @description,
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      name: :ash_async_api,
      licenses: ["MIT"],
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG* documentation),
      links: %{
        "AsyncAPI" => "https://www.asyncapi.com"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "documentation/tutorials/getting-started-with-ash-async-api.md",
        "documentation/topics/what-is-ash-async-api.md",
        "documentation/topics/transports.md",
        "documentation/dsls/DSL-AshAsyncApi.Resource.md",
        "documentation/dsls/DSL-AshAsyncApi.Domain.md"
      ],
      groups_for_extras: [
        Tutorials: ~r'documentation/tutorials',
        Topics: ~r'documentation/topics',
        DSLs: ~r'documentation/dsls'
      ],
      groups_for_modules: [
        Extensions: [
          AshAsyncApi.Resource,
          AshAsyncApi.Domain
        ],
        Spec: [
          AshAsyncApi.Spec,
          AshAsyncApi.Spec.Schema,
          AshAsyncApi.Spec.Renderer
        ],
        Runtime: [
          AshAsyncApi,
          AshAsyncApi.Router,
          AshAsyncApi.Publisher,
          AshAsyncApi.Subscriber,
          AshAsyncApi.Supervisor,
          AshAsyncApi.Envelope,
          AshAsyncApi.Address
        ],
        Transports: [
          AshAsyncApi.Transport,
          AshAsyncApi.Transport.Local,
          AshAsyncApi.Transport.Mqtt,
          AshAsyncApi.Transport.Nats,
          AshAsyncApi.Transport.Kafka
        ]
      ]
    ]
  end

  defp aliases do
    [
      docs: [
        "spark.cheat_sheets",
        "docs"
      ],
      "spark.cheat_sheets":
        "spark.cheat_sheets --extensions AshAsyncApi.Resource,AshAsyncApi.Domain",
      "spark.formatter": "spark.formatter --extensions AshAsyncApi.Resource,AshAsyncApi.Domain"
    ]
  end

  defp deps do
    [
      {:ash, "~> 3.0"},
      {:spark, "~> 2.2"},
      {:jason, "~> 1.4"},
      {:group, "~> 0.2"},
      {:ymlr, "~> 5.0", optional: true},

      # dev/test
      {:ex_doc, "~> 0.34", only: [:dev, :test], runtime: false},
      {:sourceror, "~> 1.0", only: [:dev, :test]},
      {:igniter, "~> 0.5", only: [:dev, :test]},
      {:ex_check, "~> 0.16", only: [:dev, :test]},
      {:credo, ">= 0.0.0", only: [:dev, :test], runtime: false},
      {:dialyxir, ">= 0.0.0", only: [:dev, :test], runtime: false}
    ]
  end
end
