import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type {
  ConnectorDescriptor,
  ConnectorKind,
  CredentialAccess,
} from "./consoleClient";
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

export interface PromptCredentialRequest {
  credentialId: string;
  connectorId: string;
  suggestedUsername: string;
}

export interface CredentialSaved {
  credentialId: string;
  username: string;
}

export interface DbmsConnectionRequest {
  connectorId: string;
  connectorKind: ConnectorKind;
  commandLanguage: string;
  dialect?: SqlDialect;
  endpoint: string;
  adminEndpoint?: string;
  database?: string;
  tlsMode: ConnectorDescriptor["defaultTlsMode"];
  credentialId: string;
  credentialAccess?: CredentialAccess;
}

export type ConnectionProbeStageName =
  | "service"
  | "pgPort"
  | "adminApi"
  | "initialization"
  | "authentication"
  | "catalog";

export interface ConnectionProbeStage {
  stage: ConnectionProbeStageName;
  status: "passed" | "failed" | "skipped";
  error: DbmsError | null;
}

export interface ConnectionProbe {
  ready: boolean;
  stages: ConnectionProbeStage[];
  bootstrapTicket: LocalBootstrapTicket | null;
}

export interface LocalBootstrapTicket {
  ticket: string;
  expiresInMs: number;
}

export interface BootstrapAdminRequest {
  ticket: string;
  connection: DbmsConnectionRequest;
  suggestedUsername: string;
}

export interface BootstrapAdminResult {
  success: boolean;
  user: string | null;
  error: DbmsError | null;
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
  connectorKind: ConnectorKind;
  commandLanguage: string;
  dialect: SqlDialect | null;
  endpoint: string;
  database: string;
  credentialAccess: CredentialAccess;
  mode: "native" | "plugin" | "preview";
  capabilities: DbmsCapabilities;
}

export interface DbmsCatalogObject {
  id: string | null;
  kind: string;
  schema: string;
  namespace: string | null;
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

export type DbmsCommand =
  | {
      kind: "text";
      languageId: string;
      text: string;
      params: Array<string | null>;
    }
  | { kind: "document"; languageId: string; document: unknown }
  | { kind: "arguments"; languageId: string; arguments: string[] };

export interface DbmsKeyValue {
  key: unknown;
  value: unknown;
}

export type DbmsQueryEvent =
  | { kind: "schema"; columns: DbmsQueryColumn[] }
  | { kind: "batch"; rows: Array<Array<string | null>> }
  | { kind: "documents"; documents: unknown[] }
  | { kind: "keyValues"; entries: DbmsKeyValue[] }
  | { kind: "progress"; rowsProcessed: number }
  | {
      kind: "notice";
      severity: "INFO" | "NOTICE" | "WARNING";
      sqlState: string;
      message: string;
    }
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

export type DbmsOperationKind = "backup" | "restore" | "import" | "export";
export type DbmsOperationState =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";
export type DbmsTransferFormat = "csv" | "jsonLines";

export interface DbmsOperationRecord {
  operationId: string;
  kind: DbmsOperationKind;
  state: DbmsOperationState;
  path: string;
  schema: string | null;
  table: string | null;
  startedAt: unknown | null;
  finishedAt: unknown | null;
  rows: number | null;
  bytes: number | null;
  error: DbmsError | null;
}

export interface StartDbmsOperationRequest {
  connectionId: string;
  kind: DbmsOperationKind;
  path: string;
  schema?: string;
  table?: string;
  format?: DbmsTransferFormat;
}

export interface DbmsServiceStatus {
  name: string;
  processRunning: boolean;
  windowsServiceSupported: boolean;
  dataDir: string;
  operationsRoot: string;
}

export interface DbmsClient {
  readonly mode: "desktop" | "preview";
  promptCredential(
    request: PromptCredentialRequest,
  ): Promise<CredentialSaved | null>;
  deleteCredential(credentialId: string): Promise<void>;
  probe(request: DbmsConnectionRequest): Promise<ConnectionProbe>;
  bootstrapAdmin(request: BootstrapAdminRequest): Promise<BootstrapAdminResult>;
  connect(request: DbmsConnectionRequest): Promise<DbmsConnectionSnapshot>;
  disconnect(connectionId: string): Promise<void>;
  catalog(connectionId: string): Promise<DbmsCatalogSnapshot>;
  execute(
    connectionId: string,
    command: DbmsCommand,
  ): Promise<DbmsQueryOperation>;
  cancel(requestId: string): Promise<void>;
  begin(connectionId: string): Promise<DbmsCommandResult>;
  commit(connectionId: string): Promise<DbmsCommandResult>;
  rollback(connectionId: string): Promise<DbmsCommandResult>;
  monitor(connectionId: string): Promise<DbmsMonitorSnapshot>;
  checkpoint(connectionId: string): Promise<DbmsEngineStatus>;
  operations(connectionId: string): Promise<DbmsOperationRecord[]>;
  startOperation(
    request: StartDbmsOperationRequest,
  ): Promise<DbmsOperationRecord>;
  operation(
    connectionId: string,
    operationId: string,
  ): Promise<DbmsOperationRecord>;
  cancelOperation(
    connectionId: string,
    operationId: string,
  ): Promise<DbmsOperationRecord>;
  service(connectionId: string): Promise<DbmsServiceStatus>;
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
  backup: true,
  importExport: true,
  serviceControl: false,
};

export const previewConnection: DbmsConnectionSnapshot = {
  connectionId: "preview-connection",
  connectorId: "ordadb-native",
  connectorKind: "sql",
  commandLanguage: "postgresql-sql",
  dialect: "postgresql",
  endpoint: "Preview fixture",
  database: "ordadb_preview",
  credentialAccess: "unspecified",
  mode: "preview",
  capabilities: previewCapabilities,
};

const previewCatalog: DbmsCatalogObject[] = [
  {
    id: "preview-database",
    kind: "database",
    schema: "",
    namespace: null,
    name: "ordadb_preview",
    parent: null,
    details: { mode: "Preview fixture" },
  },
  {
    id: "preview-schema-public",
    kind: "schema",
    schema: "public",
    namespace: "public",
    name: "public",
    parent: "ordadb_preview",
    details: { mode: "Preview fixture" },
  },
  {
    id: "preview-table-documents",
    kind: "table",
    schema: "public",
    namespace: "public",
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
    id: "preview-view-recent-documents",
    kind: "view",
    schema: "public",
    namespace: "public",
    name: "recent_documents",
    parent: null,
    details: { mode: "Preview fixture" },
  },
  {
    id: "preview-index-documents-search",
    kind: "index",
    schema: "public",
    namespace: "public",
    name: "documents_search_idx",
    parent: "documents",
    details: { method: "Hybrid", mode: "Preview fixture" },
  },
];

class TauriDbmsClient implements DbmsClient {
  readonly mode = "desktop";

  promptCredential(request: PromptCredentialRequest) {
    return invoke<CredentialSaved | null>("dbms_prompt_credential", { request });
  }

  deleteCredential(credentialId: string) {
    return invoke<void>("dbms_delete_credential", { credentialId });
  }

  probe(request: DbmsConnectionRequest) {
    return invoke<ConnectionProbe>("dbms_probe_connection", { request });
  }

  bootstrapAdmin(request: BootstrapAdminRequest) {
    return invoke<BootstrapAdminResult>("dbms_bootstrap_admin", { request });
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
    command: DbmsCommand,
  ): Promise<DbmsQueryOperation> {
    const stream = createQueryEventStream();
    try {
      await stream.listen();
      const started = await invoke<OperationStarted>("dbms_execute", {
        request: { connectionId, command },
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

  operations(connectionId: string) {
    return invoke<DbmsOperationRecord[]>("dbms_operations", { connectionId });
  }

  startOperation(request: StartDbmsOperationRequest) {
    return invoke<DbmsOperationRecord>("dbms_start_operation", { request });
  }

  operation(connectionId: string, operationId: string) {
    return invoke<DbmsOperationRecord>("dbms_operation", {
      connectionId,
      operationId,
    });
  }

  cancelOperation(connectionId: string, operationId: string) {
    return invoke<DbmsOperationRecord>("dbms_cancel_operation", {
      connectionId,
      operationId,
    });
  }

  service(connectionId: string) {
    return invoke<DbmsServiceStatus>("dbms_service", { connectionId });
  }
}

export class PreviewDbmsClient implements DbmsClient {
  readonly mode = "preview";
  private operationSequence = 1;
  private operationRecords: DbmsOperationRecord[] = [
    {
      operationId: "preview-operation-0",
      kind: "backup",
      state: "succeeded",
      path: "preview-demo.ordbak",
      schema: null,
      table: null,
      startedAt: "preview",
      finishedAt: "preview",
      rows: 5,
      bytes: 2048,
      error: null,
    },
  ];

  promptCredential: DbmsClient["promptCredential"] = async (request) => ({
    credentialId: request.credentialId,
    username: request.suggestedUsername,
  });

  deleteCredential: DbmsClient["deleteCredential"] = async () => {};

  probe: DbmsClient["probe"] = async () => ({
    ready: true,
    bootstrapTicket: null,
    stages: [
      "service",
      "pgPort",
      "adminApi",
      "initialization",
      "authentication",
      "catalog",
    ].map((stage) => ({
      stage: stage as ConnectionProbeStageName,
      status: "skipped" as const,
      error: null,
    })),
  });

  bootstrapAdmin: DbmsClient["bootstrapAdmin"] = async (request) => ({
    success: true,
    user: request.suggestedUsername,
    error: null,
  });

  connect: DbmsClient["connect"] = async (request) => ({
    ...previewConnection,
    connectorId: request.connectorId,
    connectorKind: request.connectorKind,
    commandLanguage: request.commandLanguage,
    dialect: request.dialect ?? null,
    database: request.database ?? previewConnection.database,
    credentialAccess: request.credentialAccess ?? "unspecified",
    capabilities: previewCapabilitiesFor(request.connectorKind),
  });

  disconnect: DbmsClient["disconnect"] = async () => {};

  catalog: DbmsClient["catalog"] = async () => ({
    connectionId: previewConnection.connectionId,
    objects: previewCatalog,
  });

  execute: DbmsClient["execute"] = async (_connectionId, command) => {
    const requestId = `preview-${Date.now()}`;
    return {
      requestId,
      events: previewQueryEvents(command),
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
      supported: true,
      reason: "Preview fixture · 不写入文件",
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

  operations: DbmsClient["operations"] = async () => {
    for (const operation of this.operationRecords) {
      if (operation.state === "queued") {
        operation.state = "succeeded";
        operation.startedAt = "preview";
        operation.finishedAt = "preview";
        operation.rows = operation.kind === "backup" ? 5 : 0;
        operation.bytes = operation.kind === "restore" ? 0 : 2048;
      }
    }
    return this.operationRecords.map((operation) => ({ ...operation }));
  };

  startOperation: DbmsClient["startOperation"] = async (request) => {
    const operation: DbmsOperationRecord = {
      operationId: `preview-operation-${this.operationSequence++}`,
      kind: request.kind,
      state: "queued",
      path: request.path,
      schema: request.schema ?? null,
      table: request.table ?? null,
      startedAt: null,
      finishedAt: null,
      rows: null,
      bytes: null,
      error: null,
    };
    this.operationRecords = [operation, ...this.operationRecords];
    return { ...operation };
  };

  operation: DbmsClient["operation"] = async (_connectionId, operationId) => {
    const operation = this.operationRecords.find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!operation) throw previewError("42704", "Preview 作业不存在");
    if (operation.state === "queued") {
      operation.state = "succeeded";
      operation.startedAt = "preview";
      operation.finishedAt = "preview";
      operation.rows = operation.kind === "backup" ? 5 : 0;
      operation.bytes = operation.kind === "restore" ? 0 : 2048;
    }
    return { ...operation };
  };

  cancelOperation: DbmsClient["cancelOperation"] = async (
    _connectionId,
    operationId,
  ) => {
    const operation = this.operationRecords.find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!operation) throw previewError("42704", "Preview 作业不存在");
    if (operation.state === "queued" || operation.state === "running") {
      operation.state = "cancelled";
      operation.finishedAt = "preview";
    }
    return { ...operation };
  };

  service: DbmsClient["service"] = async () => ({
    name: "OrdaDB Preview",
    processRunning: false,
    windowsServiceSupported: true,
    dataDir: "Preview fixture",
    operationsRoot: "Preview fixture",
  });
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
  command: DbmsCommand,
): AsyncIterable<DbmsQueryEvent> {
  await new Promise<void>((resolve) => window.setTimeout(resolve, 120));
  if (command.kind === "document") {
    yield {
      kind: "documents",
      documents: [
        {
          _id: { $oid: "64f000000000000000000001" },
          operation: command.document,
          source: "Preview fixture",
        },
      ],
    };
    yield { kind: "progress", rowsProcessed: 1 };
    yield { kind: "complete", commandTag: "MONGODB PREVIEW", durationMs: 18 };
    return;
  }
  if (command.kind === "arguments") {
    yield {
      kind: "keyValues",
      entries: [
        {
          key: command.arguments[0] ?? "COMMAND",
          value: command.arguments.slice(1),
        },
      ],
    };
    yield { kind: "progress", rowsProcessed: 1 };
    yield { kind: "complete", commandTag: "REDIS PREVIEW", durationMs: 9 };
    return;
  }
  const sql = command.text;
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
      severity: "NOTICE",
      sqlState: "00000",
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
    severity: "NOTICE",
    sqlState: "00000",
    message: "Preview fixture · 不连接真实数据库",
  };
  yield { kind: "complete", commandTag: "SELECT 5 PREVIEW", durationMs: 36 };
}

function previewCapabilitiesFor(kind: ConnectorKind): DbmsCapabilities {
  if (kind === "sql") return { ...previewCapabilities };
  return {
    ...previewCapabilities,
    transactions: false,
    explain: false,
    sessions: false,
    locks: false,
    metrics: false,
    wal: false,
    checkpoint: false,
    backup: false,
    importExport: false,
  };
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
