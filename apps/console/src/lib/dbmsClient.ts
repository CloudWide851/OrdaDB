import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { SqlDialect } from "../types";
import { isTauriRuntime } from "./tauri";

export interface DbmsError {
  sqlState: string;
  message: string;
  detail: string | null;
  hint: string | null;
  position: number | null;
  queryId: string;
}

export interface SaveCredentialRequest {
  credentialId: string;
  username: string;
  password: string;
}

export interface CredentialSaved {
  credentialId: string;
  username: string;
}

export interface DbmsConnectionRequest {
  connectorId: string;
  dialect: SqlDialect;
  endpoint: string;
  adminEndpoint?: string;
  database?: string;
  credentialId: string;
}

export interface DbmsCapabilities {
  catalog: boolean;
  transactions: boolean;
  cancel: boolean;
  explain: boolean;
  sessions: boolean;
  locks: boolean;
  metrics: boolean;
  wal: boolean;
  checkpoint: boolean;
  backup: boolean;
  importExport: boolean;
  serviceControl: boolean;
}

export interface DbmsConnectionSnapshot {
  connectionId: string;
  connectorId: string;
  dialect: SqlDialect;
  endpoint: string;
  database: string;
  mode: "native" | "plugin" | "preview";
  capabilities: DbmsCapabilities;
}

export interface DbmsCatalogObject {
  kind: string;
  schema: string;
  name: string;
  parent: string | null;
  details: unknown;
}

export interface DbmsCatalogSnapshot {
  connectionId: string;
  objects: DbmsCatalogObject[];
}

export interface DbmsQueryColumn {
  name: string;
  dataType: string;
}

export type DbmsQueryEvent =
  | { kind: "schema"; columns: DbmsQueryColumn[] }
  | { kind: "batch"; rows: Array<Array<string | null>> }
  | { kind: "progress"; rowsProcessed: number }
  | { kind: "notice"; message: string }
  | { kind: "complete"; commandTag: string; durationMs: number }
  | { kind: "error"; error: DbmsError };

interface DbmsQueryUpdate {
  requestId: string;
  event: DbmsQueryEvent;
}

interface OperationStarted {
  requestId: string;
}

export interface DbmsQueryOperation {
  requestId: string;
  events: AsyncIterable<DbmsQueryEvent>;
}

export interface DbmsCommandResult {
  commandTag: string;
}

export interface DbmsEngineStatus {
  generation: number;
  tableCount: number;
  rowCount: number;
  indexCount: number;
  durableLsn: number | null;
  dirtyPageCount: number;
  commitsSinceCheckpoint: number;
}

export interface DbmsSessionInfo {
  processId: number;
  user: string;
  database: string;
  applicationName: string | null;
  connectedAt: unknown;
  remoteAddress: string;
}

export interface DbmsQueryInfo {
  queryId: string;
  processId: number;
  sql: string;
  startedAt: unknown;
  finishedAt: unknown | null;
  rowsProcessed: number;
  outcome: unknown;
}

export interface DbmsMonitorSnapshot {
  connectionId: string;
  sessions: DbmsSessionInfo[];
  queries: DbmsQueryInfo[];
  locks: {
    singleWriter: boolean;
    activeLocks: string[];
  };
  metrics: {
    activeSessions: number;
    activeQueries: number;
    engine: DbmsEngineStatus;
  };
  storage: DbmsEngineStatus;
  wal: DbmsEngineStatus;
  backups: {
    supported: boolean;
    reason: string;
  };
  config: {
    dataDir: string;
    pgBind: string;
    adminBind: string;
    remoteRequiresTls: boolean;
  };
}

export interface DbmsClient {
  readonly mode: "desktop" | "preview";
  saveCredential(request: SaveCredentialRequest): Promise<CredentialSaved>;
  deleteCredential(credentialId: string): Promise<void>;
  connect(request: DbmsConnectionRequest): Promise<DbmsConnectionSnapshot>;
  disconnect(connectionId: string): Promise<void>;
  catalog(connectionId: string): Promise<DbmsCatalogSnapshot>;
  execute(
    connectionId: string,
    sql: string,
    params?: Array<string | null>,
  ): Promise<DbmsQueryOperation>;
  cancel(requestId: string): Promise<void>;
  begin(connectionId: string): Promise<DbmsCommandResult>;
  commit(connectionId: string): Promise<DbmsCommandResult>;
  rollback(connectionId: string): Promise<DbmsCommandResult>;
  monitor(connectionId: string): Promise<DbmsMonitorSnapshot>;
  checkpoint(connectionId: string): Promise<DbmsEngineStatus>;
}

const previewCapabilities: DbmsCapabilities = {
  catalog: true,
  transactions: true,
  cancel: true,
  explain: true,
  sessions: true,
  locks: true,
  metrics: true,
  wal: false,
  checkpoint: false,
  backup: false,
  importExport: false,
  serviceControl: false,
};

export const previewConnection: DbmsConnectionSnapshot = {
  connectionId: "preview-connection",
  connectorId: "ordadb-postgresql",
  dialect: "postgresql",
  endpoint: "Preview fixture",
  database: "ordadb_preview",
  mode: "preview",
  capabilities: previewCapabilities,
};

const previewCatalog: DbmsCatalogObject[] = [
  {
    kind: "database",
    schema: "",
    name: "ordadb_preview",
    parent: null,
    details: { mode: "Preview fixture" },
  },
  {
    kind: "schema",
    schema: "public",
    name: "public",
    parent: "ordadb_preview",
    details: { mode: "Preview fixture" },
  },
  {
    kind: "table",
    schema: "public",
    name: "documents",
    parent: null,
    details: {
      columns: [
        { name: "u:id", dataType: "Int64", nullable: false },
        { name: "u:title", dataType: "Text", nullable: false },
        { name: "u:category", dataType: "Text", nullable: true },
      ],
      ddl: "CREATE TABLE public.documents (\n  id BIGINT PRIMARY KEY,\n  title TEXT NOT NULL,\n  category TEXT\n);",
      mode: "Preview fixture",
    },
  },
  {
    kind: "view",
    schema: "public",
    name: "recent_documents",
    parent: null,
    details: { mode: "Preview fixture" },
  },
  {
    kind: "index",
    schema: "public",
    name: "documents_search_idx",
    parent: "documents",
    details: { method: "Hybrid", mode: "Preview fixture" },
  },
];

class TauriDbmsClient implements DbmsClient {
  readonly mode = "desktop";

  saveCredential(request: SaveCredentialRequest) {
    return invoke<CredentialSaved>("dbms_save_credential", { request });
  }

  deleteCredential(credentialId: string) {
    return invoke<void>("dbms_delete_credential", { credentialId });
  }

  connect(request: DbmsConnectionRequest) {
    return invoke<DbmsConnectionSnapshot>("dbms_connect", { request });
  }

  disconnect(connectionId: string) {
    return invoke<void>("dbms_disconnect", { connectionId });
  }

  catalog(connectionId: string) {
    return invoke<DbmsCatalogSnapshot>("dbms_catalog", { connectionId });
  }

  async execute(
    connectionId: string,
    sql: string,
    params: Array<string | null> = [],
  ): Promise<DbmsQueryOperation> {
    const stream = createQueryEventStream();
    try {
      await stream.listen();
      const started = await invoke<OperationStarted>("dbms_execute", {
        request: { connectionId, sql, params },
      });
      stream.select(started.requestId);
      return {
        requestId: started.requestId,
        events: stream.events(),
      };
    } catch (error) {
      stream.dispose();
      throw error;
    }
  }

  cancel(requestId: string) {
    return invoke<void>("dbms_cancel", { requestId });
  }

  begin(connectionId: string) {
    return invoke<DbmsCommandResult>("dbms_begin", { connectionId });
  }

  commit(connectionId: string) {
    return invoke<DbmsCommandResult>("dbms_commit", { connectionId });
  }

  rollback(connectionId: string) {
    return invoke<DbmsCommandResult>("dbms_rollback", { connectionId });
  }

  monitor(connectionId: string) {
    return invoke<DbmsMonitorSnapshot>("dbms_monitor", { connectionId });
  }

  checkpoint(connectionId: string) {
    return invoke<DbmsEngineStatus>("dbms_checkpoint", { connectionId });
  }
}

export class PreviewDbmsClient implements DbmsClient {
  readonly mode = "preview";

  saveCredential: DbmsClient["saveCredential"] = async (request) => ({
    credentialId: request.credentialId,
    username: request.username,
  });

  deleteCredential: DbmsClient["deleteCredential"] = async () => {};

  connect: DbmsClient["connect"] = async (request) => ({
    ...previewConnection,
    connectorId: request.connectorId,
    dialect: request.dialect,
    database: request.database ?? previewConnection.database,
  });

  disconnect: DbmsClient["disconnect"] = async () => {};

  catalog: DbmsClient["catalog"] = async () => ({
    connectionId: previewConnection.connectionId,
    objects: previewCatalog,
  });

  execute: DbmsClient["execute"] = async (_connectionId, sql) => {
    const requestId = `preview-${Date.now()}`;
    return {
      requestId,
      events: previewQueryEvents(sql),
    };
  };

  cancel: DbmsClient["cancel"] = async () => {};
  begin: DbmsClient["begin"] = async () => ({ commandTag: "BEGIN PREVIEW" });
  commit: DbmsClient["commit"] = async () => ({ commandTag: "COMMIT PREVIEW" });
  rollback: DbmsClient["rollback"] = async () => ({
    commandTag: "ROLLBACK PREVIEW",
  });

  monitor: DbmsClient["monitor"] = async () => ({
    connectionId: previewConnection.connectionId,
    sessions: [],
    queries: [],
    locks: { singleWriter: true, activeLocks: [] },
    metrics: {
      activeSessions: 0,
      activeQueries: 0,
      engine: emptyEngineStatus(),
    },
    storage: emptyEngineStatus(),
    wal: emptyEngineStatus(),
    backups: {
      supported: false,
      reason: "Preview 不连接真实数据库",
    },
    config: {
      dataDir: "",
      pgBind: "",
      adminBind: "",
      remoteRequiresTls: true,
    },
  });

  checkpoint: DbmsClient["checkpoint"] = async () => {
    throw previewError("0A000", "Preview 不执行检查点");
  };
}

let client: DbmsClient | undefined;

export function getDbmsClient(): DbmsClient {
  client ??= isTauriRuntime()
    ? new TauriDbmsClient()
    : new PreviewDbmsClient();
  return client;
}

export function normalizeDbmsError(error: unknown): DbmsError {
  if (isDbmsError(error)) return error;
  if (error instanceof Error) {
    return previewError("XX000", error.message);
  }
  return previewError("XX000", "未知数据库错误");
}

function createQueryEventStream() {
  let selectedRequestId: string | undefined;
  let unlisten: UnlistenFn | undefined;
  const buffered: DbmsQueryUpdate[] = [];
  const queue: DbmsQueryEvent[] = [];
  const waiters: Array<() => void> = [];

  const push = (update: DbmsQueryUpdate) => {
    if (!selectedRequestId) {
      buffered.push(update);
      return;
    }
    if (update.requestId !== selectedRequestId) return;
    queue.push(update.event);
    waiters.shift()?.();
  };

  return {
    async listen() {
      unlisten = await listen<DbmsQueryUpdate>("dbms://query", (event) => {
        push(event.payload);
      });
    },
    select(requestId: string) {
      selectedRequestId = requestId;
      for (const update of buffered.splice(0)) push(update);
    },
    dispose() {
      unlisten?.();
      unlisten = undefined;
      for (const resolve of waiters.splice(0)) resolve();
    },
    async *events(): AsyncIterable<DbmsQueryEvent> {
      try {
        while (true) {
          if (queue.length === 0) {
            await new Promise<void>((resolve) => waiters.push(resolve));
          }
          const event = queue.shift();
          if (!event) continue;
          yield event;
          if (event.kind === "complete" || event.kind === "error") return;
        }
      } finally {
        unlisten?.();
        unlisten = undefined;
      }
    },
  };
}

async function* previewQueryEvents(
  sql: string,
): AsyncIterable<DbmsQueryEvent> {
  await new Promise<void>((resolve) => window.setTimeout(resolve, 120));
  if (/\berror\b/i.test(sql)) {
    yield {
      kind: "error",
      error: previewError(
        "42601",
        "Preview 执行被测试关键字 ERROR 中止",
      ),
    };
    return;
  }
  if (/^\s*explain\b/i.test(sql)) {
    yield {
      kind: "schema",
      columns: [{ name: "QUERY PLAN", dataType: "text" }],
    };
    yield {
      kind: "batch",
      rows: [
        ["Preview Plan · Limit 100"],
        ["Preview Plan · Sort updated_at DESC"],
        ["Preview Plan · Seq Scan public.documents"],
      ],
    };
    yield {
      kind: "notice",
      message: "Preview 执行计划，不连接真实数据库",
    };
    yield { kind: "complete", commandTag: "EXPLAIN PREVIEW", durationMs: 12 };
    return;
  }
  yield {
    kind: "schema",
    columns: [
      { name: "id", dataType: "bigint" },
      { name: "title", dataType: "text" },
      { name: "category", dataType: "text" },
      { name: "score", dataType: "double" },
      { name: "updated_at", dataType: "timestamp" },
    ],
  };
  yield {
    kind: "batch",
    rows: [
      ["101", "WAL checkpoint overview", "database", "0.982", "2026-07-24 10:42"],
      ["102", "Index maintenance", "operations", "0.941", "2026-07-24 10:38"],
      ["103", "Role audit", "security", "0.917", "2026-07-24 10:31"],
      ["104", "Query plan notes", "database", "0.891", "2026-07-24 10:18"],
      ["105", "Backup policy", "operations", "0.864", "2026-07-24 10:07"],
    ],
  };
  yield { kind: "progress", rowsProcessed: 5 };
  yield {
    kind: "notice",
    message: "Preview fixture · 不连接真实数据库",
  };
  yield { kind: "complete", commandTag: "SELECT 5 PREVIEW", durationMs: 36 };
}

function emptyEngineStatus(): DbmsEngineStatus {
  return {
    generation: 0,
    tableCount: 0,
    rowCount: 0,
    indexCount: 0,
    durableLsn: null,
    dirtyPageCount: 0,
    commitsSinceCheckpoint: 0,
  };
}

function isDbmsError(value: unknown): value is DbmsError {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<DbmsError>;
  return (
    typeof candidate.sqlState === "string" &&
    typeof candidate.message === "string" &&
    typeof candidate.queryId === "string"
  );
}

function previewError(sqlState: string, message: string): DbmsError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: `preview-${Date.now()}`,
  };
}
