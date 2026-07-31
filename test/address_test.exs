defmodule AshAsyncApi.AddressTest do
  use ExUnit.Case, async: true

  doctest AshAsyncApi.Address

  alias AshAsyncApi.Address

  describe "segment lists" do
    test "literals and fields are joined by the delimiter" do
      compiled = Address.compile(["helpdesk", "tickets", :id, "events"], delimiter: "/")

      assert compiled.template == "helpdesk/tickets/{id}/events"
      assert compiled.params == [:id]
    end

    test "the same declaration takes the shape of whichever bus carries it" do
      segments = ["helpdesk", "tickets", :id, "events"]

      assert Address.template(segments, delimiter: "/") == "helpdesk/tickets/{id}/events"
      assert Address.template(segments, delimiter: ".") == "helpdesk.tickets.{id}.events"
      assert Address.template(segments, delimiter: ":") == "helpdesk:tickets:{id}:events"
    end

    test "a delimiter of more than one character works" do
      assert Address.template(["a", :b, "c"], delimiter: "::") == "a::{b}::c"
    end

    test "defaults to /" do
      assert Address.template(["a", :b]) == "a/{b}"
    end

    test "a relationship path becomes one parameter named after the path" do
      compiled = Address.compile(["tickets", [:organization, :id]])

      assert compiled.template == "tickets/{organization_id}"
      assert compiled.params == [:organization_id]
      assert compiled.param_paths == %{organization_id: [:organization, :id]}
    end

    test "a deep relationship path joins every hop into the name" do
      compiled = Address.compile([[:ticket, :organization, :slug]])

      assert compiled.params == [:ticket_organization_slug]
      assert compiled.param_paths[:ticket_organization_slug] == [:ticket, :organization, :slug]
    end

    test "a {name, path} pair names the parameter explicitly" do
      compiled = Address.compile(["tickets", {:org, [:organization, :id]}])

      assert compiled.template == "tickets/{org}"
      assert compiled.param_paths == %{org: [:organization, :id]}
    end

    test "a {name, field} pair accepts a bare field as the path" do
      compiled = Address.compile([{:ticket, :id}])

      assert compiled.param_paths == %{ticket: [:id]}
    end

    test "a plain field's path is the field itself" do
      assert Address.compile([:id]).param_paths == %{id: [:id]}
    end

    test "literals and parameters can interleave freely" do
      compiled =
        Address.compile(["helpdesk", :org_id, "tickets", :id, "comments", [:author, :id]],
          delimiter: "."
        )

      assert compiled.template == "helpdesk.{org_id}.tickets.{id}.comments.{author_id}"
      assert compiled.params == [:org_id, :id, :author_id]
    end

    test "an all-literal address has no parameters" do
      compiled = Address.compile(["helpdesk", "audit"])

      assert compiled.template == "helpdesk/audit"
      assert compiled.params == []
      refute Address.templated?(compiled)
    end

    test "a single segment needs no delimiter at all" do
      assert Address.template(["helpdesk"]) == "helpdesk"
      assert Address.template([:id]) == "{id}"
    end

    test "rejects a segment that is not a legal shape" do
      assert_raise ArgumentError, ~r/Invalid address segment 42/, fn ->
        Address.compile(["tickets", 42])
      end

      assert_raise ArgumentError, ~r/must contain only atoms/, fn ->
        Address.compile(["tickets", [:organization, "id"]])
      end
    end
  end

  describe "string addresses" do
    test "braces mark the parameters" do
      compiled = Address.compile("helpdesk/tickets/{ticket_id}/events")

      assert compiled.params == [:ticket_id]
      assert compiled.template == "helpdesk/tickets/{ticket_id}/events"
    end

    test "the delimiter is auto-detected" do
      assert Address.compile("a/b/{c}").delimiter == "/"
      assert Address.compile("a.b.{c}").delimiter == "."
      assert Address.compile("{c}").delimiter == nil
    end

    test "prefers / when both are present" do
      assert Address.compile("a.b/c/{d}").delimiter == "/"
    end

    test "express a parameter inside a segment, which a list cannot" do
      compiled = Address.compile("tickets/id-{ticket_id}/events")

      assert {:ok, "tickets/id-42/events"} = Address.interpolate(compiled, %{ticket_id: 42})
      assert {:ok, %{ticket_id: "42"}} = Address.match(compiled, "tickets/id-42/events")
    end

    test "a parameter's source path defaults to the parameter name" do
      assert Address.compile("tickets/{id}").param_paths == %{id: [:id]}
    end
  end

  describe "interpolate/2" do
    test "accepts atom and string keys" do
      assert {:ok, "t/1/e"} = Address.interpolate(["t", :id, "e"], %{id: 1})
      assert {:ok, "t/1/e"} = Address.interpolate(["t", :id, "e"], %{"id" => 1})
    end

    test "stringifies values" do
      assert {:ok, "t/42"} = Address.interpolate(["t", :id], %{id: 42})
      assert {:ok, "t/urgent"} = Address.interpolate(["t", :id], %{id: :urgent})
      assert {:ok, "t/1.5"} = Address.interpolate(["t", :id], %{id: Decimal.new("1.5")})
    end

    test "treats nil as missing, since it cannot appear in an address" do
      assert {:error, {:missing_params, [:id]}} = Address.interpolate(["t", :id], %{id: nil})
    end

    test "reports every missing parameter at once" do
      assert {:error, {:missing_params, [:a, :b]}} = Address.interpolate([:a, :b], %{})
    end

    test "a relationship parameter is keyed by its derived name" do
      assert {:ok, "tickets/acme"} =
               Address.interpolate(["tickets", [:organization, :id]], %{organization_id: "acme"})
    end

    test "an all-literal address needs no values" do
      assert {:ok, "helpdesk/audit"} = Address.interpolate(["helpdesk", "audit"], %{})
    end
  end

  describe "match/2" do
    test "extracts parameter values" do
      assert {:ok, %{id: "42"}} = Address.match(["t", :id, "e"], "t/42/e")
    end

    test "a parameter matches exactly one segment" do
      assert :error = Address.match(["t", :id], "t/42/nested")
      assert :error = Address.match(["t", :id, "e"], "t//e")
    end

    test "matches multiple parameters" do
      assert {:ok, %{tenant: "acme", id: "7"}} =
               Address.match([:tenant, "tickets", :id], "acme/tickets/7")
    end

    test "respects a dot delimiter" do
      compiled = Address.compile(["t", :id, "e"], delimiter: ".")

      assert {:ok, %{id: "42"}} = Address.match(compiled, "t.42.e")
      assert :error = Address.match(compiled, "t.42.nested.e")
    end

    test "does not match a partial address" do
      assert :error = Address.match(["t", :id, "e"], "prefix/t/42/e")
      assert :error = Address.match(["t", :id, "e"], "t/42/e/suffix")
    end

    test "an all-literal address matches only itself" do
      assert {:ok, %{}} = Address.match(["helpdesk", "audit"], "helpdesk/audit")
      assert :error = Address.match(["helpdesk", "audit"], "helpdesk/audits")
    end

    test "a literal containing regex metacharacters is matched literally" do
      compiled = Address.compile(["a.b", :id])

      assert {:ok, %{id: "1"}} = Address.match(compiled, "a.b/1")
      assert :error = Address.match(compiled, "axb/1")
    end
  end

  describe "to_filter/2" do
    test "single-level wildcards" do
      assert Address.to_filter(["t", :id, "e"], {:single, "+"}) == "t/+/e"
      assert Address.to_filter([:a, :b], {:single, "+"}) == "+/+"

      assert Address.compile(["t", :id, "e"], delimiter: ".")
             |> Address.to_filter({:single, "*"}) == "t.*.e"
    end

    test "multi-level wildcards truncate at the first parameter" do
      assert Address.to_filter(["t", :id, "e"], :multi_level) == "t/#"

      assert Address.compile(["t", :id, "e"], delimiter: ".")
             |> Address.to_filter(:multi_level) == "t.>"
    end

    test "exact yields the literal prefix with no trailing delimiter" do
      assert Address.to_filter(["t", :id, "e"], :exact) == "t"

      assert Address.compile(["helpdesk", "tickets", :id], delimiter: ".")
             |> Address.to_filter(:exact) == "helpdesk.tickets"
    end

    test "an all-literal address is its own filter" do
      assert Address.to_filter(["helpdesk", "audit"], {:single, "+"}) == "helpdesk/audit"
      assert Address.to_filter(["helpdesk", "audit"], :exact) == "helpdesk/audit"
    end
  end

  describe "prefix/1" do
    test "is the literal part before the first parameter" do
      assert Address.prefix(["helpdesk", "tickets", :id, "events"]) == "helpdesk/tickets"
      assert Address.prefix(["helpdesk", "audit"]) == "helpdesk/audit"
      assert Address.prefix([:id, "events"]) == ""
    end
  end

  describe "round trip" do
    test "interpolating then matching returns the original values, on any delimiter" do
      segments = ["helpdesk", "tickets", :ticket_id, "comments", :comment_id]
      params = %{ticket_id: "abc", comment_id: "123"}

      for delimiter <- ["/", ".", ":", "::"] do
        compiled = Address.compile(segments, delimiter: delimiter)

        assert {:ok, address} = Address.interpolate(compiled, params)
        assert {:ok, ^params} = Address.match(compiled, address)
      end
    end
  end
end
