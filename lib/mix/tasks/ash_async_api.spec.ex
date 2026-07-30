defmodule Mix.Tasks.AshAsyncApi.Spec do
  @moduledoc """
  Write a router's AsyncAPI 3.0 document to a file.

      mix ash_async_api.spec --router MyApp.AsyncApiRouter

  Useful in CI to check the document into the repository, or to feed it to the
  AsyncAPI toolchain for validation, docs and client generation.

  ## Options

    * `--router` — the router module. Required unless exactly one router exists in the
      project, in which case it is found automatically.
    * `--output` / `-o` — where to write. Defaults to `asyncapi.json`. The extension
      picks the format: `.yaml`/`.yml` produce YAML, anything else JSON.
    * `--server` — restrict the document to these servers. Repeatable.
    * `--include-local` — include servers using `AshAsyncApi.Transport.Local`, which
      are omitted by default as an implementation detail.
    * `--check` — do not write; exit non-zero if the file on disk differs from what
      would be generated. For CI.
  """

  @shortdoc "Write a router's AsyncAPI 3.0 document to a file"

  use Mix.Task

  @requirements ["app.config"]

  @switches [
    router: :string,
    output: :string,
    server: :keep,
    include_local: :boolean,
    check: :boolean
  ]

  @aliases [o: :output, r: :router]

  @impl true
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, switches: @switches, aliases: @aliases)

    router = router(opts)
    output = opts[:output] || "asyncapi.json"

    contents = generate(router, output, opts)

    if opts[:check] do
      check(output, contents)
    else
      write(output, contents, router)
    end
  end

  defp generate(router, output, opts) do
    generate_opts =
      []
      |> put_servers(Keyword.get_values(opts, :server))
      |> Keyword.put(:include_local?, opts[:include_local] || false)

    if yaml?(output) do
      AshAsyncApi.Spec.to_yaml(router, generate_opts)
    else
      AshAsyncApi.Spec.to_json(router, generate_opts)
    end
  end

  defp put_servers(opts, []), do: opts

  defp put_servers(opts, servers),
    do: Keyword.put(opts, :servers, Enum.map(servers, &String.to_atom/1))

  defp yaml?(output), do: Path.extname(output) in [".yaml", ".yml"]

  defp write(output, contents, router) do
    output |> Path.dirname() |> File.mkdir_p!()
    File.write!(output, contents)

    Mix.shell().info([
      :green,
      "* generated ",
      :reset,
      output,
      " from ",
      inspect(router)
    ])
  end

  defp check(output, contents) do
    existing = if File.exists?(output), do: File.read!(output), else: nil

    cond do
      is_nil(existing) ->
        Mix.raise("""
        #{output} does not exist, but --check was given.

        Run `mix ash_async_api.spec` to create it.
        """)

      String.trim(existing) != String.trim(contents) ->
        Mix.raise("""
        #{output} is out of date.

        Run `mix ash_async_api.spec` and commit the result.
        """)

      true ->
        Mix.shell().info([:green, "* ", :reset, output, " is up to date"])
    end
  end

  defp router(opts) do
    case opts[:router] do
      nil -> discover_router()
      router -> parse_module(router)
    end
  end

  defp parse_module(name) do
    module = Module.concat([name])

    case Code.ensure_compiled(module) do
      {:module, module} ->
        if function_exported?(module, :__ash_async_api_config__, 0) do
          module
        else
          Mix.raise("#{inspect(module)} is not an AshAsyncApi.Router.")
        end

      {:error, reason} ->
        Mix.raise("Could not load #{inspect(module)}: #{inspect(reason)}")
    end
  end

  defp discover_router do
    case routers() do
      [router] ->
        router

      [] ->
        Mix.raise("""
        No AshAsyncApi router found in this project.

        Define one:

            defmodule MyApp.AsyncApiRouter do
              use AshAsyncApi.Router, domains: [MyApp.MyDomain]
            end
        """)

      routers ->
        Mix.raise("""
        This project has more than one AshAsyncApi router, so --router is required:

        #{Enum.map_join(routers, "\n", &"    mix ash_async_api.spec --router #{inspect(&1)}")}
        """)
    end
  end

  defp routers do
    otp_app = Mix.Project.config()[:app]

    case :application.get_key(otp_app, :modules) do
      {:ok, modules} ->
        Enum.filter(modules, fn module ->
          Code.ensure_loaded?(module) and function_exported?(module, :__ash_async_api_config__, 0)
        end)

      _ ->
        []
    end
  end
end
