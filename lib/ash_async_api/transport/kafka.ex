defmodule AshAsyncApi.Transport.Kafka do
  @moduledoc """
  Kafka transport, built on [`brod`](https://hex.pm/packages/brod).

  Kafka is the odd one out. It has no wildcards and no hierarchy — a topic is a flat
  name — so a templated address cannot be a topic. Instead the literal prefix of the
  address becomes the topic and the parameters become the **message key**:

      channel :ticket_events, "helpdesk.tickets.{ticket_id}"
      #  topic: "helpdesk.tickets"
      #  key:   the ticket id

  Using the parameters as the key is not a workaround, it is the right thing: Kafka
  guarantees ordering within a partition, and keying by entity id puts every event for
  one ticket on one partition, in order. Address matching still works on the way in,
  because the key is recombined with the topic to reconstruct the full address.

  ## Setup

      {:brod, "~> 4.0"}

      servers do
        server :kafka, "kafka.example.com:9092" do
          protocol :kafka
          transport AshAsyncApi.Transport.Kafka
          transport_opts [
            client_id: :helpdesk_kafka,
            group_id: "helpdesk",
            endpoints: [{"kafka.example.com", 9092}]
          ]
        end
      end

  ## Options

    * `:client_id` — the `brod` client id. Derived from the router and server name when
      omitted.
    * `:group_id` — the consumer group. **Required for consuming.** Kafka's consumer
      group protocol is what stops every node from processing every message.
    * `:endpoints` — `{host, port}` tuples. Derived from the server's `host` when
      omitted.
    * `:client_config` / `:group_config` / `:consumer_config` — passed through to
      `brod`.
    * `:partitioner` — `:hash` (default), `:random`, or a function. `:hash` keeps a
      given key's messages in order.

  ## Delivery scope

  This transport reports `delivery_scope/0` as `:local`. Every node in a consumer group
  gets its own partitions, so a message that arrives on node A is *already* the only
  copy in the cluster, and `AshAsyncApi.PubSub` should not re-broadcast it to nodes B
  and C — they will get their own messages from their own partitions. Subscribers
  therefore see the messages belonging to the partitions their node owns.
  """

  use AshAsyncApi.Transport

  alias AshAsyncApi.Transport.Context

  @impl true
  def wildcard_style, do: :exact

  @impl true
  def delivery_scope, do: :local

  @impl true
  def validate_opts(server, opts) do
    # Configuration mistakes are reported before the missing-dependency error, because
    # "you forgot :group_id" is the more specific and more surprising of the two.
    cond do
      is_nil(opts[:group_id]) ->
        {:error,
         """
         #{inspect(__MODULE__)} requires a :group_id, because Kafka uses consumer groups \
         to decide which node handles which partition. Without one, every node would \
         process every message.

             server #{inspect(server.name)}, #{inspect(server.host)} do
               transport #{inspect(__MODULE__)}
               transport_opts [group_id: "my-app"]
             end
         """}

      not Code.ensure_loaded?(:brod) ->
        {:error,
         """
         The :brod library is required to use #{inspect(__MODULE__)}. Add it to your mix.exs:

             {:brod, "~> 4.0"}
         """}

      true ->
        :ok
    end
  end

  @impl true
  def child_spec(%Context{} = context) do
    %{
      id: Context.process_name(context),
      start: {AshAsyncApi.Transport.Kafka.Connection, :start_link, [context]},
      type: :supervisor
    }
  end

  @impl true
  def publish(%Context{} = context, address, body, opts) do
    AshAsyncApi.Transport.Kafka.Connection.publish(context, address, body, opts)
  end

  @impl true
  def subscribe(%Context{} = context, filter) do
    AshAsyncApi.Transport.Kafka.Connection.subscribe(context, filter)
  end

  @impl true
  def unsubscribe(_context, _filter) do
    # Leaving a consumer group mid-flight rebalances the whole group; stopping the
    # transport is the honest way to stop consuming.
    {:error, :unsupported}
  end

  @doc """
  Split an address into a Kafka topic and message key.

  The literal prefix becomes the topic; whatever remains becomes the key.

      iex> AshAsyncApi.Transport.Kafka.split_address("helpdesk.tickets.42", "helpdesk.tickets.{id}")
      {"helpdesk.tickets", "42"}

      iex> AshAsyncApi.Transport.Kafka.split_address("helpdesk.audit", "helpdesk.audit")
      {"helpdesk.audit", nil}
  """
  @spec split_address(String.t(), String.t() | AshAsyncApi.Address.t()) ::
          {String.t(), String.t() | nil}
  def split_address(address, template) do
    prefix = AshAsyncApi.Address.prefix(template)

    cond do
      prefix == "" ->
        {address, nil}

      address == prefix ->
        {prefix, nil}

      true ->
        separator = separator(template)
        rest = address |> String.replace_prefix(prefix, "") |> String.trim_leading(separator)

        {prefix, presence(rest)}
    end
  end

  @doc """
  Rebuild the full address from a Kafka topic and key.

  The inverse of `split_address/2`, used on the inbound path so that address matching
  and parameter extraction work exactly as they do for every other transport.
  """
  @spec join_address(String.t(), String.t() | nil, String.t() | AshAsyncApi.Address.t() | nil) ::
          String.t()
  def join_address(topic, key, template \\ nil)
  def join_address(topic, nil, _template), do: topic
  def join_address(topic, "", _template), do: topic

  def join_address(topic, key, template) do
    topic <> separator(template || topic) <> key
  end

  defp separator(template) do
    compiled =
      case template do
        %AshAsyncApi.Address{} = compiled -> compiled
        template when is_binary(template) -> AshAsyncApi.Address.compile(template)
      end

    compiled.delimiter || "."
  end

  defp presence(""), do: nil
  defp presence(value), do: value
end
