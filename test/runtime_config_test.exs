defmodule AshAsyncApi.RuntimeConfigTest do
  @moduledoc """
  Per-server runtime configuration: `servers: [name: :disabled]` skips a transport and
  silently drops its publishes; `servers: [name: [transport_opts: ...]]` merges over the
  compile-time declaration.
  """

  use AshAsyncApi.RouterCase, async: false

  import ExUnit.CaptureLog

  alias AshAsyncApi.Test.Rec.Widget
  alias AshAsyncApi.Test.RecordingTransport
  alias AshAsyncApi.Test.RecRouter

  defp create_widget! do
    Widget
    |> Ash.Changeset.for_create(:create, %{label: "w"})
    |> Ash.create!()
  end

  test "runtime transport_opts merge over the compile-time declaration" do
    start_router!(RecRouter,
      start_transports?: true,
      servers: [rec: [transport_opts: [queue_group: "runtime", extra: 1]]]
    )

    assert RecordingTransport.started?(:rec)
    assert AshAsyncApi.Supervisor.active_servers(RecRouter) == MapSet.new([:rec])

    %{context: context} = RecordingTransport.state(:rec)
    assert context.server.transport_opts[:compile_time] == true
    assert context.server.transport_opts[:queue_group] == "runtime"
    assert context.server.transport_opts[:extra] == 1
  end

  test "an active server receives publishes" do
    start_router!(RecRouter, start_transports?: true)
    RecRouter.subscribe(:events)

    widget = create_widget!()

    assert_message(%{address: "rec.widget.created." <> _})

    %{publishes: [{address, body, _opts}]} = RecordingTransport.state(:rec)
    assert address == "rec.widget.created.#{widget.id}"
    assert %{"payload" => %{"label" => "w"}} = Jason.decode!(body)
  end

  test "a disabled server publishes nothing, silently, while PubSub keeps working" do
    start_router!(RecRouter, start_transports?: true, servers: [rec: :disabled])

    refute RecordingTransport.started?(:rec)
    assert AshAsyncApi.Supervisor.active_servers(RecRouter) == MapSet.new()

    RecRouter.subscribe(:events)

    log =
      capture_log(fn ->
        create_widget!()

        # In-cluster delivery is independent of the broker, so subscribers still see it.
        assert_message(%{address: "rec.widget.created." <> _})
      end)

    refute log =~ "failed to publish"
  end

  test "start_transports?: false silences every server the same way" do
    start_router!(RecRouter)

    assert AshAsyncApi.Supervisor.active_servers(RecRouter) == MapSet.new()

    RecRouter.subscribe(:events)

    log =
      capture_log(fn ->
        create_widget!()
        assert_message(%{address: "rec.widget.created." <> _})
      end)

    refute log =~ "failed to publish"
  end
end
