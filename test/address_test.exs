defmodule AshAsyncApi.AddressTest do
  use ExUnit.Case, async: true

  doctest AshAsyncApi.Address

  alias AshAsyncApi.Address

  describe "compile/2" do
    test "detects the separator from the template" do
      assert Address.compile("a/b/{c}").separator == "/"
      assert Address.compile("a.b.{c}").separator == "."
      assert Address.compile("{c}").separator == nil
    end

    test "prefers / when both separators are present" do
      assert Address.compile("a.b/c/{d}").separator == "/"
    end

    test "collects parameters in order" do
      assert Address.compile("{a}/x/{b}/{c}").params == [:a, :b, :c]
    end

    test "an address with no parameters has none" do
      compiled = Address.compile("helpdesk/audit")

      assert compiled.params == []
      refute Address.templated?(compiled)
    end
  end

  describe "interpolate/2" do
    test "accepts atom and string keys" do
      assert {:ok, "t/1/e"} = Address.interpolate("t/{id}/e", %{id: 1})
      assert {:ok, "t/1/e"} = Address.interpolate("t/{id}/e", %{"id" => 1})
    end

    test "stringifies the values it is given" do
      assert {:ok, "t/42"} = Address.interpolate("t/{id}", %{id: 42})
      assert {:ok, "t/urgent"} = Address.interpolate("t/{id}", %{id: :urgent})
      assert {:ok, "t/1.5"} = Address.interpolate("t/{id}", %{id: Decimal.new("1.5")})
    end

    test "treats a nil value as missing, since it cannot appear in an address" do
      assert {:error, {:missing_params, [:id]}} = Address.interpolate("t/{id}", %{id: nil})
    end

    test "reports every missing parameter at once" do
      assert {:error, {:missing_params, [:a, :b]}} = Address.interpolate("{a}/{b}", %{})
    end

    test "leaves an untemplated address alone" do
      assert {:ok, "helpdesk/audit"} = Address.interpolate("helpdesk/audit", %{})
    end
  end

  describe "match/2" do
    test "extracts parameter values" do
      assert {:ok, %{id: "42"}} = Address.match("t/{id}/e", "t/42/e")
    end

    test "a parameter matches exactly one segment" do
      assert :error = Address.match("t/{id}", "t/42/nested")
      assert :error = Address.match("t/{id}/e", "t//e")
    end

    test "matches multiple parameters" do
      assert {:ok, %{tenant: "acme", id: "7"}} =
               Address.match("{tenant}/tickets/{id}", "acme/tickets/7")
    end

    test "respects a dot separator" do
      assert {:ok, %{id: "42"}} = Address.match("t.{id}.e", "t.42.e")
      assert :error = Address.match("t.{id}", "t.42.nested")
    end

    test "does not match a partial address" do
      assert :error = Address.match("t/{id}/e", "prefix/t/42/e")
      assert :error = Address.match("t/{id}/e", "t/42/e/suffix")
    end

    test "an untemplated address matches only itself" do
      assert {:ok, %{}} = Address.match("helpdesk/audit", "helpdesk/audit")
      assert :error = Address.match("helpdesk/audit", "helpdesk/audits")
    end

    test "regex metacharacters in the template are literal" do
      assert {:ok, %{id: "1"}} = Address.match("a.b/{id}", "a.b/1")
      assert :error = Address.match("a.b/{id}", "axb/1")
    end
  end

  describe "to_filter/2" do
    test "single-level wildcards" do
      assert Address.to_filter("t/{id}/e", {:single, "+"}) == "t/+/e"
      assert Address.to_filter("t.{id}.e", {:single, "*"}) == "t.*.e"
      assert Address.to_filter("{a}/{b}", {:single, "+"}) == "+/+"
    end

    test "multi-level wildcards truncate at the first parameter" do
      assert Address.to_filter("t/{id}/e", :multi_level) == "t/#"
      assert Address.to_filter("t.{id}.e", :multi_level) == "t.>"
    end

    test "exact yields the literal prefix with no trailing separator" do
      assert Address.to_filter("t/{id}/e", :exact) == "t"
      assert Address.to_filter("helpdesk.tickets.{id}", :exact) == "helpdesk.tickets"
    end

    test "an untemplated address is its own filter" do
      assert Address.to_filter("helpdesk/audit", {:single, "+"}) == "helpdesk/audit"
      assert Address.to_filter("helpdesk/audit", :exact) == "helpdesk/audit"
    end
  end

  describe "prefix/1" do
    test "is the literal part before the first parameter" do
      assert Address.prefix("helpdesk/tickets/{id}/events") == "helpdesk/tickets"
      assert Address.prefix("helpdesk/audit") == "helpdesk/audit"
      assert Address.prefix("{id}/events") == ""
    end
  end

  describe "round trip" do
    test "interpolating then matching returns the original values" do
      template = "helpdesk/tickets/{ticket_id}/comments/{comment_id}"
      params = %{ticket_id: "abc", comment_id: "123"}

      assert {:ok, address} = Address.interpolate(template, params)
      assert {:ok, ^params} = Address.match(template, address)
    end
  end
end
