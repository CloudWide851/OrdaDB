import { describe, expect, it } from "vitest";
import { formatSqlForDialect, getSqlDialect } from "./dialects";

describe("dialect-aware SQL formatting", () => {
  it("formats supported keywords without rewriting quoted or commented text", () => {
    expect(
      formatSqlForDialect(
        "select 'from' as \"where\" -- select\nfrom items where id = $1",
        getSqlDialect("postgresql"),
      ),
    ).toBe("SELECT 'from' AS \"where\" -- select\nFROM items WHERE id = $1");
  });

  it("preserves PostgreSQL dollar bodies and escaped SQL Server brackets", () => {
    expect(
      formatSqlForDialect(
        "create function f() as $body$ begin select 1; end $body$",
        getSqlDialect("postgresql"),
      ),
    ).toBe("CREATE function f() AS $body$ begin select 1; end $body$");
    expect(
      formatSqlForDialect(
        "select [from]]where] from [items]",
        getSqlDialect("sqlServer"),
      ),
    ).toBe("SELECT [from]]where] FROM [items]");
  });
});
