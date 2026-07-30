defmodule AshAsyncApi.Test.DslHelpers do
  @moduledoc """
  Helpers for asserting on compile-time DSL errors.

  Spark runs verifiers from an `@after_verify` hook, and Elixir reports an exception
  raised there as a compiler diagnostic rather than letting it propagate. So
  `assert_raise` around a `defmodule` never fires. `assert_dsl_error/2` compiles the
  module, captures what the compiler said, and asserts against that — which is exactly
  what the person who made the mistake will see.
  """

  import ExUnit.Assertions

  @doc """
  Compile a module body and assert the DSL error it produces matches `expected`.

  The body is given as a quoted block, usually via `quote do ... end`.
  """
  defmacro assert_dsl_error(expected, do: body) do
    quote do
      AshAsyncApi.Test.DslHelpers.do_assert_dsl_error(
        unquote(expected),
        unquote(Macro.escape(body, unquote: true))
      )
    end
  end

  @doc false
  def do_assert_dsl_error(expected, quoted) do
    output =
      ExUnit.CaptureIO.capture_io(:stderr, fn ->
        Code.compile_quoted(quoted)
      end)

    assert output =~ "Spark.Error.DslError",
           """
           Expected compiling the module to produce a Spark.Error.DslError, but it did not.

           Compiler output:
           #{output}
           """

    assert output =~ expected,
           """
           The DSL error did not match #{inspect(expected)}.

           Compiler output:
           #{output}
           """

    output
  end
end
