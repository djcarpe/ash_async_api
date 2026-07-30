defmodule AshAsyncApi.Transport.Kafka.Subscriber do
  @moduledoc """
  The `:brod_group_subscriber_v2` callback module.

  Recombines the topic and the message key back into the full channel address before
  handing the message over, so that everything above the transport — address matching,
  parameter extraction, action dispatch — behaves the same as it does for MQTT or NATS.
  """

  # Implements the `:brod_group_subscriber_v2` behaviour. Declaring it with
  # `@behaviour` is not possible: `:brod` is an optional dependency, so the behaviour
  # module is usually absent at compile time.
  require Logger

  alias AshAsyncApi.Transport.Kafka

  @doc "`c::brod_group_subscriber_v2.init/2`"
  def init(_group_init_data, init_data), do: {:ok, init_data}

  @doc "`c::brod_group_subscriber_v2.handle_message/2`"
  def handle_message(message, %{context: context, topic: topic} = state) do
    key = kafka_field(message, :key)
    address = Kafka.join_address(topic, key)

    AshAsyncApi.Transport.deliver(context, address, kafka_field(message, :value),
      headers: headers(message),
      metadata: %{
        topic: topic,
        key: key,
        offset: kafka_field(message, :offset),
        timestamp: kafka_field(message, :ts)
      }
    )

    # Committing after delivery gives at-least-once: a crash before this line means
    # the message is redelivered.
    {:ok, :commit, state}
  rescue
    error ->
      Logger.error("""
      AshAsyncApi failed to handle a Kafka message on #{inspect(state.topic)}:
      #{Exception.format(:error, error, __STACKTRACE__)}
      """)

      {:ok, :commit, state}
  end

  defp headers(message) do
    case kafka_field(message, :headers) do
      headers when is_list(headers) ->
        Map.new(headers, fn {key, value} -> {to_string(key), value} end)

      _ ->
        %{}
    end
  end

  # `kafka_message` is an Erlang record, so fields come out positionally. Reading them
  # by name keeps this readable and survives record layout changes in brod.
  defp kafka_field(message, field) do
    fields = [:offset, :key, :value, :ts_type, :ts, :headers]

    case Enum.find_index(fields, &(&1 == field)) do
      nil -> nil
      index -> elem(message, index + 1)
    end
  end
end
