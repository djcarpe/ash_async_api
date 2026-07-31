defmodule AshAsyncApi.Resource.Verifiers.VerifyChannels do
  @moduledoc """
  Checks channel addresses at compile time: that channel names are unique, that every
  segment is a legal shape, that field and relationship segments actually exist on the
  resource, and that declared `parameter` blocks match the address.

  Relationship paths are the ones worth catching early — `[:organisation, :id]` with the
  wrong spelling would otherwise surface as an unfillable address at publish time, long
  after the mistake.
  """

  use Spark.Dsl.Verifier

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    channels = Verifier.get_entities(dsl, [:async_api, :channels])

    with :ok <- verify_unique_names(dsl, channels) do
      Enum.reduce_while(channels, :ok, fn channel, :ok ->
        case verify_channel(dsl, channel) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp verify_unique_names(dsl, channels) do
    channels
    |> Enum.frequencies_by(& &1.name)
    |> Enum.find(fn {_name, count} -> count > 1 end)
    |> case do
      nil ->
        :ok

      {name, _count} ->
        {:error,
         error(
           dsl,
           [:async_api, :channels, name],
           "Multiple channels are named #{inspect(name)}. Channel names must be unique."
         )}
    end
  end

  defp verify_channel(_dsl, %{segments: nil}), do: :ok

  defp verify_channel(dsl, channel) do
    with :ok <- verify_segments(dsl, channel) do
      verify_parameters(dsl, channel)
    end
  end

  defp verify_segments(_dsl, %{segments: segments}) when is_binary(segments), do: :ok

  defp verify_segments(dsl, %{segments: segments} = channel) when is_list(segments) do
    Enum.reduce_while(segments, :ok, fn segment, :ok ->
      case verify_segment(dsl, channel, segment) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify_segments(dsl, channel) do
    {:error,
     error(dsl, path(channel), """
     Invalid address #{inspect(channel.segments)}.

     A channel address is a list of segments, or a string:

         channel #{inspect(channel.name)}, ["helpdesk", "tickets", :id, "events"]
         channel #{inspect(channel.name)}, "helpdesk/tickets/{id}/events"
     """)}
  end

  defp verify_segment(_dsl, _channel, literal) when is_binary(literal), do: :ok

  defp verify_segment(dsl, channel, field) when is_atom(field) and not is_nil(field) do
    verify_path(dsl, channel, [field])
  end

  defp verify_segment(dsl, channel, path) when is_list(path) do
    if path != [] and Enum.all?(path, &is_atom/1) do
      verify_path(dsl, channel, path)
    else
      {:error,
       error(dsl, path(channel), """
       Invalid address segment #{inspect(path)} on channel #{inspect(channel.name)}.

       A list segment is a relationship path and must contain only atoms, e.g \
       #{inspect([:organization, :id])}.
       """)}
    end
  end

  defp verify_segment(dsl, channel, {name, source}) when is_atom(name) do
    verify_path(dsl, channel, List.wrap(source))
  end

  defp verify_segment(dsl, channel, other) do
    {:error,
     error(dsl, path(channel), """
     Invalid address segment #{inspect(other)} on channel #{inspect(channel.name)}.

     A segment is one of:

         "a literal"                        a literal string
         :field                             a field on the resource
         [:relationship, :field]            a relationship traversal
         {:name, [:relationship, :field]}   a traversal with an explicit parameter name
     """)}
  end

  # Walk the path through the resource's relationships. Resolution is kept separate from
  # error construction, because past the first hop we are looking at another *module* rather
  # than a DSL state, and only the originating resource can raise a useful DslError.
  defp verify_path(dsl, channel, path) do
    case resolve_path(dsl, path) do
      :ok ->
        :ok

      {:error, {:unknown_field, field, available}} ->
        {:error,
         error(dsl, path(channel), """
         Channel #{inspect(channel.name)} addresses #{inspect(field)}, which is not a field on \
         this resource.

         Available: #{inspect(available)}
         """)}

      {:error, {:unknown_relationship, relationship, available}} ->
        {:error,
         error(dsl, path(channel), """
         Channel #{inspect(channel.name)} addresses #{inspect(relationship)}, which is not a \
         relationship on this resource.

         Available relationships: #{inspect(available)}
         """)}

      {:error, {:unresolved, destination, rest}} ->
        {:error,
         error(dsl, path(channel), """
         Channel #{inspect(channel.name)} addresses #{inspect(path)}, but #{inspect(rest)} does \
         not resolve on #{inspect(destination)}.

         Available there: #{inspect(field_names(destination))}
         """)}
    end
  end

  defp resolve_path(resource, [field]) do
    if known_field?(resource, field) do
      :ok
    else
      {:error, {:unknown_field, field, field_names(resource)}}
    end
  end

  defp resolve_path(resource, [relationship | rest]) do
    case Ash.Resource.Info.relationship(resource, relationship) do
      nil ->
        {:error, {:unknown_relationship, relationship, relationship_names(resource)}}

      %{destination: destination} ->
        # The destination is a separate module and may not be compiled yet; when it is not,
        # the rest of the path goes unchecked rather than producing a bogus error.
        if match?({:module, _}, Code.ensure_compiled(destination)) do
          case resolve_path(destination, rest) do
            :ok -> :ok
            {:error, _} -> {:error, {:unresolved, destination, rest}}
          end
        else
          :ok
        end
    end
  end

  defp verify_parameters(dsl, %{segments: segments} = channel) do
    address_params =
      segments
      |> AshAsyncApi.Address.compile()
      |> AshAsyncApi.Address.params()

    declared = Enum.map(channel.parameters, & &1.name)

    case declared -- address_params do
      [] ->
        :ok

      undeclared ->
        {:error,
         error(dsl, path(channel), """
         Channel #{inspect(channel.name)} documents parameter(s) #{inspect(undeclared)} that \
         its address does not contain.

             address parameters: #{inspect(address_params)}

         Parameter names are derived from the address: `:id` becomes `:id`, and \
         #{inspect([:organization, :id])} becomes `:organization_id`.
         """)}
    end
  rescue
    # A malformed segment already produced a better error above.
    ArgumentError -> :ok
  end

  defp known_field?(dsl, field) do
    not is_nil(Ash.Resource.Info.field(dsl, field))
  end

  defp field_names(dsl),
    do: dsl |> Ash.Resource.Info.fields() |> Enum.map(& &1.name) |> Enum.sort()

  defp relationship_names(dsl),
    do: dsl |> Ash.Resource.Info.relationships() |> Enum.map(& &1.name) |> Enum.sort()

  defp path(channel), do: [:async_api, :channels, channel.name]

  defp error(dsl, path, message) do
    Spark.Error.DslError.exception(
      module: Verifier.get_persisted(dsl, :module),
      path: path,
      message: message
    )
  end
end
