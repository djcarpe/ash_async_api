defmodule AshAsyncApi.PubSubTest do
  use AshAsyncApi.RouterCase, async: false

  alias AshAsyncApi.PubSub
  alias AshAsyncApi.Test.Router

  setup do
    start_router!(Router)
    :ok
  end

  describe "subscribe/3 and broadcast/2" do
    test "a channel subscriber receives messages on that channel" do
      :ok = PubSub.subscribe(Router, :ticket_events)

      envelope = envelope(channel: :ticket_events, address: "helpdesk/tickets/1/events")
      :ok = PubSub.broadcast(Router, envelope)

      assert_receive {:ash_async_api, received}, 500
      assert received.id == envelope.id
    end

    test "an address subscriber receives only that address" do
      :ok = PubSub.subscribe(Router, "helpdesk/tickets/1/events")

      PubSub.broadcast(
        Router,
        envelope(channel: :ticket_events, address: "helpdesk/tickets/2/events")
      )

      refute_receive {:ash_async_api, _}, 200

      PubSub.broadcast(
        Router,
        envelope(channel: :ticket_events, address: "helpdesk/tickets/1/events")
      )

      assert_receive {:ash_async_api, _}, 500
    end

    test "a channel subscriber gets messages for every address on the channel" do
      :ok = PubSub.subscribe(Router, :ticket_events)

      for id <- 1..3 do
        PubSub.broadcast(
          Router,
          envelope(channel: :ticket_events, address: "helpdesk/tickets/#{id}/events")
        )
      end

      for id <- 1..3 do
        assert_receive {:ash_async_api, %{address: address}}, 500
        assert address == "helpdesk/tickets/#{id}/events"
      end
    end

    test "one broadcast reaches a channel subscriber and an address subscriber exactly once" do
      :ok = PubSub.subscribe(Router, :ticket_events)
      :ok = PubSub.subscribe(Router, "helpdesk/tickets/1/events")

      PubSub.broadcast(
        Router,
        envelope(channel: :ticket_events, address: "helpdesk/tickets/1/events")
      )

      # Two subscriptions in this one process, so two deliveries — and no more.
      assert_receive {:ash_async_api, _}, 500
      assert_receive {:ash_async_api, _}, 500
      refute_receive {:ash_async_api, _}, 200
    end

    test "unsubscribing stops delivery" do
      :ok = PubSub.subscribe(Router, :ticket_events)
      :ok = PubSub.unsubscribe(Router, :ticket_events)

      PubSub.broadcast(
        Router,
        envelope(channel: :ticket_events, address: "helpdesk/tickets/1/events")
      )

      refute_receive {:ash_async_api, _}, 200
    end

    test "a subscription dies with its process" do
      test_pid = self()

      subscriber =
        spawn(fn ->
          PubSub.subscribe(Router, :ticket_events)
          send(test_pid, :subscribed)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :subscribed, 500
      assert PubSub.subscriber_count(Router, :ticket_events) == 1

      send(subscriber, :stop)
      ref = Process.monitor(subscriber)
      assert_receive {:DOWN, ^ref, :process, _, _}, 500

      # Group cleans up on death, but asynchronously.
      assert eventually(fn -> PubSub.subscriber_count(Router, :ticket_events) == 0 end)
    end
  end

  describe "subscribers/2" do
    test "returns pids with the metadata they subscribed with" do
      :ok = PubSub.subscribe(Router, :ticket_events, %{role: :auditor})

      assert [{pid, %{role: :auditor}}] = PubSub.subscribers(Router, :ticket_events)
      assert pid == self()
    end

    test "supports a prefix query across addresses" do
      :ok = PubSub.subscribe(Router, "helpdesk/tickets/1/events")
      :ok = PubSub.subscribe(Router, "helpdesk/tickets/2/events")

      assert length(PubSub.subscribers(Router, "address/helpdesk/tickets/")) == 2
    end
  end

  describe "subscriber_count/2" do
    test "counts channel and address subscriptions separately" do
      :ok = PubSub.subscribe(Router, :ticket_events)

      assert PubSub.subscriber_count(Router, :ticket_events) == 1
      assert PubSub.subscriber_count(Router, "helpdesk/tickets/1/events") == 0
    end
  end

  describe "keys" do
    test "channel keys are scoped to the router, so two routers cannot cross-deliver" do
      refute PubSub.channel_key(AshAsyncApi.Test.Router, :ticket_events) ==
               PubSub.channel_key(AshAsyncApi.Test.LoopbackRouter, :ticket_events)
    end

    test "address keys never end in a separator, which Group reserves for prefix queries" do
      refute String.ends_with?(PubSub.address_key("helpdesk/tickets/"), "/")
      assert PubSub.address_key("helpdesk/tickets/") == PubSub.address_key("helpdesk/tickets")
    end
  end

  defp envelope(attrs), do: AshAsyncApi.Envelope.new(attrs)

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end
end
