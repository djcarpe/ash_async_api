defmodule AshAsyncApi.RouterCase do
  @moduledoc """
  Starts a router's supervision tree for the duration of a test.

  The `Group` instance a router owns is global, so tests that publish or subscribe need
  one running. Starting it per test with `start_supervised!` means each test gets a
  clean subscription registry.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import AshAsyncApi.RouterCase
    end
  end

  @doc """
  Start a router under the test supervisor.

  Passes `start_transports?: false` unless told otherwise, because most tests do not
  need a broker connection and starting one would slow every test down.
  """
  def start_router!(router, overrides \\ []) do
    overrides = Keyword.put_new(overrides, :start_transports?, false)

    ExUnit.Callbacks.start_supervised!(
      %{
        id: router,
        start: {AshAsyncApi.Supervisor, :start_link, [router, overrides]},
        type: :supervisor
      },
      restart: :temporary
    )

    router
  end

  @doc """
  Assert that an `{:ash_async_api, envelope}` message arrives, and return the envelope.
  """
  defmacro assert_message(pattern \\ quote(do: _), timeout \\ 500) do
    quote do
      assert_receive {:ash_async_api, %AshAsyncApi.Envelope{} = envelope}, unquote(timeout)
      assert unquote(pattern) = envelope
      envelope
    end
  end

  @doc """
  Assert that no message arrives on any subscription.
  """
  defmacro refute_message(timeout \\ 200) do
    quote do
      refute_receive {:ash_async_api, %AshAsyncApi.Envelope{}}, unquote(timeout)
    end
  end
end
