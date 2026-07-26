import type { SqlDialect } from "../types";

export interface SqlDialectDescriptor {
  id: SqlDialect;
  label: string;
  parameterExample: string;
  quoteExample: string;
  paginationExample: string;
  keywords: readonly string[];
}

export const sqlDialects: readonly SqlDialectDescriptor[] = [
  {
    id: "postgresql",
    label: "PostgreSQL",
    parameterExample: "$1",
    quoteExample: '"customer_id"',
    paginationExample: "LIMIT 100",
    keywords: ["RETURNING", "ON CONFLICT", "JSONB", "UUID", "LIMIT"],
  },
  {
    id: "mysql",
    label: "MySQL",
    parameterExample: "?",
    quoteExample: "`customer_id`",
    paginationExample: "LIMIT 100",
    keywords: ["TINYINT", "MEDIUMINT", "BLOB", "DATETIME", "LIMIT"],
  },
  {
    id: "sqlite",
    label: "SQLite",
    parameterExample: "?",
    quoteExample: '"customer_id"',
    paginationExample: "LIMIT 100",
    keywords: ["INTEGER", "TEXT", "BLOB", "DATETIME", "LIMIT"],
  },
  {
    id: "sqlServer",
    label: "SQL Server",
    parameterExample: "@p1",
    quoteExample: "[customer_id]",
    paginationExample: "TOP 100",
    keywords: ["TOP", "NVARCHAR", "UNIQUEIDENTIFIER", "DATETIME", "ORDER BY"],
  },
] as const;

const descriptorById = new Map(
  sqlDialects.map((dialect) => [dialect.id, dialect]),
);

export function getSqlDialect(
  dialect: SqlDialect,
): SqlDialectDescriptor {
  const descriptor = descriptorById.get(dialect);
  if (!descriptor) {
    return sqlDialects[0];
  }
  return descriptor;
}

const commonFormatKeywords = [
  "select",
  "distinct",
  "from",
  "where",
  "insert",
  "into",
  "values",
  "update",
  "set",
  "delete",
  "create",
  "alter",
  "drop",
  "table",
  "index",
  "join",
  "inner",
  "left",
  "on",
  "group",
  "order",
  "by",
  "having",
  "limit",
  "offset",
  "fetch",
  "first",
  "next",
  "rows",
  "only",
  "top",
  "as",
  "and",
  "or",
  "not",
  "null",
  "primary",
  "key",
  "unique",
  "default",
  "check",
  "returning",
  "conflict",
  "do",
  "nothing",
] as const;

export function formatSqlForDialect(
  sql: string,
  dialect: SqlDialectDescriptor,
): string {
  const keywords = new Set([
    ...commonFormatKeywords,
    ...dialect.keywords.flatMap((keyword) =>
      keyword.toLowerCase().split(/\s+/u),
    ),
  ]);
  let formatted = "";
  let index = 0;

  while (index < sql.length) {
    if (sql.startsWith("--", index)) {
      const lineEnd = sql.indexOf("\n", index);
      if (lineEnd === -1) {
        formatted += sql.slice(index);
        break;
      }
      formatted += sql.slice(index, lineEnd + 1);
      index = lineEnd + 1;
      continue;
    }
    if (sql.startsWith("/*", index)) {
      const commentEnd = sql.indexOf("*/", index + 2);
      const end = commentEnd === -1 ? sql.length : commentEnd + 2;
      formatted += sql.slice(index, end);
      index = end;
      continue;
    }

    const character = sql[index];
    if (character === "$") {
      const delimiter = sql
        .slice(index)
        .match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/u)?.[0];
      if (delimiter) {
        const bodyEnd = sql.indexOf(delimiter, index + delimiter.length);
        const end = bodyEnd === -1 ? sql.length : bodyEnd + delimiter.length;
        formatted += sql.slice(index, end);
        index = end;
        continue;
      }
    }
    if (character === "'" || character === '"' || character === "`" || character === "[") {
      const closing = character === "[" ? "]" : character;
      const start = index;
      index += 1;
      while (index < sql.length) {
        if (sql[index] === closing) {
          if (sql[index + 1] === closing) {
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        index += 1;
      }
      formatted += sql.slice(start, index);
      continue;
    }

    if (/[A-Za-z_]/u.test(character)) {
      const start = index;
      index += 1;
      while (index < sql.length && /[A-Za-z0-9_$]/u.test(sql[index])) {
        index += 1;
      }
      const token = sql.slice(start, index);
      formatted += keywords.has(token.toLowerCase())
        ? token.toUpperCase()
        : token;
      continue;
    }

    formatted += character;
    index += 1;
  }

  return formatted;
}
