import type { ConsoleSettingsV2 } from "../../lib/consoleClient";
import type {
  DbmsCatalogObject,
  DbmsClient,
  DbmsCommand,
  DbmsConnectionSnapshot,
  DbmsError,
  DbmsOperationRecord,
} from "../../lib/dbmsClient";
import { normalizeDbmsError } from "../../lib/dbmsClient";
import type { ResultBufferLimits } from "../../lib/resultBuffer";
import type { StoreGet, StoreSet } from "./context";
import type { WorkbenchState } from "./types";
export function setCatalog(
  set: StoreSet,
  get: StoreGet,
  catalog: DbmsCatalogObject[],
) {
  const current = get().selectedObject;
  const selected =
    catalog.find((object) => catalogObjectIdentity(object) === current) ??
    catalog.find((object) => object.name === current) ??
    catalog.find((object) => object.kind === "table") ??
    catalog[0] ??
    null;
  set({
    catalog,
    selectedObject: selected ? catalogObjectIdentity(selected) : "",
    selectedCatalogObject: selected,
  });
}

export function setQueryError(set: StoreSet, error: DbmsError) {
  set({
    queryState: "error",
    error,
    errorMessage: error.message,
    activeRequestId: null,
    activeResultTab: "logs",
    notice: `命令失败 · ${error.sqlState}`,
  });
}

export async function runTransaction(
  action: "begin" | "commit" | "rollback",
  dbms: DbmsClient,
  set: StoreSet,
  get: StoreGet,
) {
  const connection = get().connection;
  if (!connection) {
    set({ dataSourceOpen: true, notice: "请先连接数据源" });
    return;
  }
  if (!connection.capabilities.transactions) {
    set({
      connectionError: localError("0A000", "当前数据源不支持事务"),
      notice: "当前数据源不支持事务",
    });
    return;
  }
  try {
    const result = await dbms[action](connection.connectionId);
    set({
      transactionActive: action === "begin",
      notice: result.commandTag,
    });
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    set({ connectionError: normalized, notice: normalized.message });
  }
}

export function localError(sqlState: string, message: string): DbmsError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: `console-${Date.now()}`,
  };
}

export function withTimeout<T>(
  operation: PromiseLike<T>,
  timeoutMs: number,
  createError: () => DbmsError,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(createError()), timeoutMs);
    void Promise.resolve(operation).then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export function runConnectionStep<T>(
  label: string,
  operation: PromiseLike<T>,
  get: StoreGet,
) {
  const timeoutMs = get().settings.connections.timeoutMs;
  return withTimeout(operation, timeoutMs, () =>
    localError("08001", `${label}超过 ${Math.ceil(timeoutMs / 1_000)} 秒`),
  );
}

export function queryTimeoutError(timeoutMs: number) {
  return localError(
    "57014",
    `命令超过 ${Math.ceil(timeoutMs / 1_000)} 秒，已请求取消`,
  );
}

export function resultBufferLimits(settings: ConsoleSettingsV2): ResultBufferLimits {
  return {
    pageRows: settings.results.pageSize,
    maxRows: settings.results.residentRowLimit,
    maxBytes: settings.results.residentMemoryBytes,
  };
}

export function catalogObjectIdentity(object: DbmsCatalogObject) {
  return object.id ?? `${object.kind}:${object.schema}:${object.name}`;
}

const MAX_CONNECTOR_TEXT_BYTES = 1024 * 1024;
const MAX_CONNECTOR_COMMAND_ARGUMENTS = 4096;

export function buildDbmsCommand(
  connection: DbmsConnectionSnapshot,
  input: string,
): DbmsCommand {
  if (new TextEncoder().encode(input).byteLength > MAX_CONNECTOR_TEXT_BYTES) {
    throw localError("54000", "命令超过 1 MiB 上限");
  }
  switch (connection.connectorKind) {
    case "sql":
      return {
        kind: "text",
        languageId: connection.commandLanguage,
        text: input,
        params: [],
      };
    case "document": {
      let document: unknown;
      try {
        document = JSON.parse(input) as unknown;
      } catch (error) {
        const message = error instanceof Error ? error.message : "无效 JSON";
        throw localError("22023", `MongoDB 命令必须是有效 JSON：${message}`);
      }
      if (!document || typeof document !== "object" || Array.isArray(document)) {
        throw localError("22023", "MongoDB 命令必须是 JSON 对象");
      }
      return {
        kind: "document",
        languageId: connection.commandLanguage,
        document,
      };
    }
    case "keyValue":
      return {
        kind: "arguments",
        languageId: connection.commandLanguage,
        arguments: parseRedisArguments(input),
      };
  }
}

export function parseRedisArguments(input: string): string[] {
  const arguments_: string[] = [];
  let current = "";
  let quote: '"' | "'" | null = null;
  let escaped = false;
  let tokenStarted = false;
  for (const character of input) {
    if (escaped) {
      current +=
        character === "n"
          ? "\n"
          : character === "r"
            ? "\r"
            : character === "t"
              ? "\t"
              : character;
      escaped = false;
      tokenStarted = true;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      tokenStarted = true;
      continue;
    }
    if (quote) {
      if (character === quote) quote = null;
      else current += character;
      tokenStarted = true;
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      tokenStarted = true;
      continue;
    }
    if (/\s/u.test(character)) {
      if (tokenStarted) {
        arguments_.push(current);
        current = "";
        tokenStarted = false;
      }
      continue;
    }
    current += character;
    tokenStarted = true;
  }
  if (escaped || quote) {
    throw localError("22023", "Redis 命令包含未结束的转义或引号");
  }
  if (tokenStarted) arguments_.push(current);
  if (arguments_.length === 0) {
    throw localError("22023", "Redis 命令至少需要一个参数");
  }
  if (arguments_.length > MAX_CONNECTOR_COMMAND_ARGUMENTS) {
    throw localError("54000", "Redis 命令参数超过 4096 个上限");
  }
  return arguments_;
}

export function appendStructuredValues<T>(
  current: T[],
  incoming: T[],
  currentBytes: number,
  currentDroppedItems: number,
  settings: ConsoleSettingsV2,
) {
  const items = [...current];
  let bytes = currentBytes;
  let accepted = 0;
  for (const item of incoming) {
    const itemBytes = estimateJsonBytes(item);
    if (
      items.length >= settings.results.residentRowLimit ||
      bytes + itemBytes > settings.results.residentMemoryBytes
    ) {
      break;
    }
    items.push(item);
    bytes += itemBytes;
    accepted += 1;
  }
  return {
    items,
    bytes,
    droppedItems: currentDroppedItems + incoming.length - accepted,
  };
}

export function estimateJsonBytes(value: unknown) {
  try {
    return new TextEncoder().encode(JSON.stringify(value) ?? "null").byteLength;
  } catch {
    return MAX_CONNECTOR_TEXT_BYTES;
  }
}

export function resultItemCount(state: WorkbenchState) {
  return (
    state.resultBuffer.totalRows +
    state.documentResults.length +
    state.keyValueResults.length +
    state.droppedStructuredItems
  );
}

export function supportsMonitor(connection: DbmsConnectionSnapshot) {
  const capabilities = connection.capabilities;
  return (
    capabilities.sessions ||
    capabilities.locks ||
    capabilities.metrics ||
    capabilities.wal
  );
}

export function loadCatalog(dbms: DbmsClient, connection: DbmsConnectionSnapshot) {
  return connection.capabilities.catalog
    ? dbms.catalog(connection.connectionId)
    : Promise.resolve({ connectionId: connection.connectionId, objects: [] });
}

export function loadMonitor(dbms: DbmsClient, connection: DbmsConnectionSnapshot) {
  return supportsMonitor(connection)
    ? dbms.monitor(connection.connectionId)
    : Promise.resolve(null);
}

const READ_ONLY_SQL_KEYWORDS = new Set([
  "DESC",
  "DESCRIBE",
  "EXPLAIN",
  "SELECT",
  "SHOW",
  "TABLE",
  "VALUES",
]);

export function requiresDangerousWriteConfirmation(sql: string) {
  return !READ_ONLY_SQL_KEYWORDS.has(leadingSqlKeyword(sql));
}

export function leadingSqlKeyword(sql: string) {
  let offset = 0;
  while (offset < sql.length) {
    while (offset < sql.length && /\s/u.test(sql[offset])) offset += 1;
    if (sql.startsWith("--", offset)) {
      const newline = sql.indexOf("\n", offset + 2);
      offset = newline === -1 ? sql.length : newline + 1;
      continue;
    }
    if (sql.startsWith("/*", offset)) {
      const end = sql.indexOf("*/", offset + 2);
      offset = end === -1 ? sql.length : end + 2;
      continue;
    }
    break;
  }
  return sql.slice(offset).match(/^[A-Za-z]+/u)?.[0]?.toUpperCase() ?? "";
}

export function replaceOperation(
  operations: DbmsOperationRecord[],
  incoming: DbmsOperationRecord,
) {
  return [
    incoming,
    ...operations.filter(
      (operation) => operation.operationId !== incoming.operationId,
    ),
  ];
}
