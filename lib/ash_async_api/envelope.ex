defmodule AshAsyncApi.Envelope do
  @moduledoc """
  The unit that travels through AshAsyncApi.

  An envelope is what a transport encodes on the way out and decodes on the way
  in, and what subscriber processes receive. It carries the AsyncAPI-visible parts
  of a message (`payload`, `headers`, `content_type`, `correlation_id`) alongside
  the routing information AshAsyncApi needs (`channel`, `address`, `operation`).

  Processes subscribed via `AshAsyncApi.subscribe/2` receive
  `{:ash_async_api, envelope}` messages.
  """

  @derive {Inspect, except: [:private]}
  defstruct [
    :id,
    :channel,
    :address,
    :operation,
    :message,
    :payload,
    :correlation_id,
    :reply_to,
    :resource,
    :action,
    :server,
    :router,
    headers: %{},
    params: %{},
    content_type: "application/json",
    metadata: %{},
    private: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          channel: atom() | nil,
          address: String.t() | nil,
          operation: atom() | nil,
          message: String.t() | nil,
          payload: term(),
          correlation_id: String.t() | nil,
          reply_to: String.t() | nil,
          resource: module() | nil,
          action: atom() | nil,
          server: atom() | nil,
          router: module() | nil,
          headers: map(),
          params: %{atom() => String.t()},
          content_type: String.t(),
          metadata: map(),
          private: map()
        }

  @doc """
  Build an envelope, generating an `:id` when one is not given.
  """
  @spec new(keyword() | map()) :: t()
  def new(attrs) do
    attrs = Map.new(attrs)

    __MODULE__
    |> struct(attrs)
    |> Map.update!(:id, fn
      nil -> generate_id()
      id -> id
    end)
  end

  @doc """
  Put a header, stringifying the key.
  """
  @spec put_header(t(), atom() | String.t(), term()) :: t()
  def put_header(%__MODULE__{headers: headers} = envelope, key, value) do
    %{envelope | headers: Map.put(headers, to_string(key), value)}
  end

  @doc """
  Fetch a header, accepting an atom or string key.
  """
  @spec get_header(t(), atom() | String.t(), term()) :: term()
  def get_header(%__MODULE__{headers: headers}, key, default \\ nil) do
    Map.get(headers, to_string(key), default)
  end

  @doc """
  Build the reply envelope for a request, carrying the correlation id across.

  Returns `{:error, :no_reply_address}` when the request did not ask for a reply.
  """
  @spec reply(t(), term(), keyword()) :: {:ok, t()} | {:error, :no_reply_address}
  def reply(request, payload, opts \\ [])

  def reply(%__MODULE__{reply_to: nil}, _payload, _opts), do: {:error, :no_reply_address}

  def reply(%__MODULE__{} = request, payload, opts) do
    {:ok,
     new(
       channel: opts[:channel] || request.channel,
       address: request.reply_to,
       operation: request.operation,
       message: opts[:message],
       payload: payload,
       correlation_id: request.correlation_id || request.id,
       content_type: request.content_type,
       resource: request.resource,
       router: request.router,
       server: request.server,
       params: request.params,
       headers: opts[:headers] || %{}
     )}
  end

  @doc """
  The map form of an envelope that gets serialized onto the wire.

  Only the fields that are meaningful to a remote consumer are included — routing
  internals like `:router` and `:private` stay local.
  """
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = envelope) do
    %{
      "id" => envelope.id,
      "channel" => envelope.channel && Atom.to_string(envelope.channel),
      "address" => envelope.address,
      "operation" => envelope.operation && Atom.to_string(envelope.operation),
      "message" => envelope.message,
      "payload" => envelope.payload,
      "headers" => envelope.headers,
      "contentType" => envelope.content_type,
      "correlationId" => envelope.correlation_id,
      "replyTo" => envelope.reply_to
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) or value == %{} end)
    |> Map.new()
  end

  @doc """
  Rebuild an envelope from its wire form.

  Accepts either a full envelope map (as produced by `to_wire/1`) or a bare
  payload map, which is treated as the payload itself. This is what lets
  AshAsyncApi consume topics published by systems that know nothing about it.
  """
  @spec from_wire(map(), keyword()) :: t()
  def from_wire(wire, overrides \\ [])

  def from_wire(%{"payload" => payload} = wire, overrides) do
    [
      id: wire["id"],
      channel: maybe_atom(wire["channel"]),
      address: wire["address"],
      operation: maybe_atom(wire["operation"]),
      message: wire["message"],
      payload: payload,
      headers: wire["headers"] || %{},
      content_type: wire["contentType"] || "application/json",
      correlation_id: wire["correlationId"],
      reply_to: wire["replyTo"]
    ]
    |> Keyword.reject(fn {_key, value} -> is_nil(value) end)
    |> Keyword.merge(Keyword.reject(overrides, fn {_k, v} -> is_nil(v) end))
    |> new()
  end

  def from_wire(payload, overrides) do
    overrides
    |> Keyword.reject(fn {_key, value} -> is_nil(value) end)
    |> Keyword.put_new(:payload, payload)
    |> new()
  end

  defp maybe_atom(nil), do: nil
  defp maybe_atom(value) when is_atom(value), do: value

  defp maybe_atom(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp generate_id do
    Ash.UUID.generate()
  end
end
