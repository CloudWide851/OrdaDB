import { invoke } from "@tauri-apps/api/core";
import type { SqlDialect } from "../types";
import { isTauriRuntime } from "./tauri";

export interface ConsoleSettingsV2 {
  formatVersion: 2;
  appearance: {
    theme: "system" | "light" | "dark";
    zoomPercent: number;
    uiFontSize: number;
    dataFontSize: number;
    density: "compact" | "comfortable";
    reduceMotion: boolean;
    hideEmptyCatalog: boolean;
  };
  editor: {
    fontFamily: string;
    fontSize: number;
    tabSize: number;
    wordWrap: "off" | "on" | "bounded";
    minimap: boolean;
    formatOnSave: boolean;
  };
  files: {
    recoveryPolicy: "prompt" | "never" | "automatic";
    autoSave: "off" | "afterDelay" | "onFocusChange";
    autoSaveDelayMs: number;
    confirmDirtyClose: boolean;
    reopenLastProject: boolean;
  };
  results: {
    pageSize: number;
    residentRowLimit: number;
    residentMemoryBytes: number;
    nullDisplay: string;
    queryTimeoutMs: number;
  };
  connections: {
    timeoutMs: number;
    autoReconnectLocal: boolean;
    confirmDangerousWrites: boolean;
  };
  ai: {
    provider: "openai" | "openaiCompatible" | "ollama";
    model: string;
    endpoint?: string;
    reasoning: "low" | "medium" | "high";
    dataSharing: "schemaOnly" | "askEachTime" | "allowSamples";
    credentialId?: string;
  };
}

export interface FileRevision {
  sizeBytes: number;
  modifiedAtMs: number;
  sha256: string;
}

export type DocumentLocator =
  | { kind: "workspace"; rootPath: string; path: string }
  | { kind: "external"; path: string }
  | { kind: "untitled"; id: string };

export interface SqlDocument {
  locator: Exclude<DocumentLocator, { kind: "untitled" }>;
  path: string;
  name: string;
  content: string;
  revision: FileRevision;
}

export interface OpenSqlDocument {
  locator: DocumentLocator;
  path: string;
  name: string;
  content: string;
  revision: FileRevision | null;
  savedContent: string;
  dirty: boolean;
  conflict: boolean;
}

export interface UntitledSqlDocument extends OpenSqlDocument {
  locator: Extract<DocumentLocator, { kind: "untitled" }>;
  revision: null;
}

export interface WorkspaceEntry {
  path: string;
  name: string;
  kind: "directory" | "sqlFile";
  depth: number;
}

export interface WorkspaceSnapshot {
  formatVersion: 1;
  rootPath: string;
  entries: WorkspaceEntry[];
}

export interface WorkspaceDraft {
  path: string;
  locator?: DocumentLocator;
  name?: string;
  content: string;
  baseRevision: FileRevision | null;
}

export interface WorkspaceSessionV1 {
  formatVersion: 1;
  rootPath: string | null;
  activePath: string | null;
  openDocuments: WorkspaceDraft[];
}

export interface RecentFileEntry {
  locator: Exclude<DocumentLocator, { kind: "untitled" }>;
  name: string;
  openedAtMs: number;
}

export interface SaveDocumentAsRequest {
  content: string;
  suggestedName: string;
}

export type DataSourceKind =
  | "ordadbNative"
  | "postgresql"
  | "mysql"
  | "sqlite"
  | "sqlServer"
  | "mongodb"
  | "redis"
  | "mariadb"
  | "clickhouse"
  | "oracle";

export type ConnectorKind = "sql" | "document" | "keyValue";
export type ConnectorEditorMode = "sql" | "json" | "plaintext";

export interface ConnectorDescriptor {
  dataSourceKind: DataSourceKind;
  connectorId: string;
  connectorKind: ConnectorKind;
  commandLanguage: string;
  editorMode: ConnectorEditorMode;
  dialect?: SqlDialect;
  displayName: string;
  defaultEndpoint: string;
  defaultAdminEndpoint?: string;
  defaultDatabase?: string;
  defaultTlsMode:
    | "disable"
    | "prefer"
    | "require"
    | "verifyCa"
    | "verifyFull";
  logoAsset:
    | "ordadb"
    | "postgresql"
    | "mysql"
    | "sqlite"
    | "sql-server"
    | "mongodb"
    | "redis"
    | "mariadb"
    | "clickhouse"
    | "oracle";
}

export type CredentialAccess = "unspecified" | "readOnly" | "readWrite";

export interface ConnectionProfileV3 {
  formatVersion: 3;
  profileId: string;
  label: string;
  dataSourceKind: DataSourceKind;
  connectorId: string;
  connectorKind: ConnectorKind;
  commandLanguage: string;
  dialect?: SqlDialect;
  endpoint: string;
  adminEndpoint?: string;
  database?: string;
  tlsMode: ConnectorDescriptor["defaultTlsMode"];
  credentialId: string;
  credentialAccess: CredentialAccess;
  autoReconnect: boolean;
}

export interface ConsoleBootstrap {
  settings: ConsoleSettingsV2;
  recovery: WorkspaceSessionV1 | null;
  recentFiles: RecentFileEntry[];
  connectionProfiles: ConnectionProfileV3[];
  connectorDescriptors: ConnectorDescriptor[];
}

export interface ConsoleClient {
  readonly mode: "desktop" | "preview";
  bootstrap(): Promise<ConsoleBootstrap>;
  saveSettings(settings: ConsoleSettingsV2): Promise<ConsoleSettingsV2>;
  pickWorkspace(): Promise<WorkspaceSnapshot | null>;
  pickDocument(): Promise<SqlDocument | null>;
  openWorkspace(rootPath: string): Promise<WorkspaceSnapshot>;
  openDocument(rootPath: string, path: string): Promise<SqlDocument>;
  openExternalDocument(path: string): Promise<SqlDocument>;
  newDocument(
    rootPath: string,
    parentPath: string,
    fileName: string,
  ): Promise<SqlDocument>;
  saveDocument(
    rootPath: string,
    document: OpenSqlDocument,
    force?: boolean,
  ): Promise<SqlDocument>;
  saveExternalDocument(
    document: OpenSqlDocument,
    force?: boolean,
  ): Promise<SqlDocument>;
  saveDocumentAs(request: SaveDocumentAsRequest): Promise<SqlDocument | null>;
  renameEntry(
    rootPath: string,
    path: string,
    newName: string,
  ): Promise<WorkspaceSnapshot>;
  trashEntry(rootPath: string, path: string): Promise<WorkspaceSnapshot>;
  saveSession(session: WorkspaceSessionV1): Promise<void>;
  saveConnectionProfile(
    profile: ConnectionProfileV3,
  ): Promise<ConnectionProfileV3[]>;
  deleteConnectionProfile(profileId: string): Promise<ConnectionProfileV3[]>;
}

export const defaultConsoleSettings: ConsoleSettingsV2 = {
  formatVersion: 2,
  appearance: {
    theme: "system",
    zoomPercent: 100,
    uiFontSize: 11,
    dataFontSize: 12,
    density: "compact",
    reduceMotion: false,
    hideEmptyCatalog: true,
  },
  editor: {
    fontFamily: "Cascadia Mono",
    fontSize: 12,
    tabSize: 2,
    wordWrap: "off",
    minimap: false,
    formatOnSave: false,
  },
  files: {
    recoveryPolicy: "prompt",
    autoSave: "off",
    autoSaveDelayMs: 1_000,
    confirmDirtyClose: true,
    reopenLastProject: false,
  },
  results: {
    pageSize: 256,
    residentRowLimit: 10_000,
    residentMemoryBytes: 16 * 1024 * 1024,
    nullDisplay: "NULL",
    queryTimeoutMs: 30_000,
  },
  connections: {
    timeoutMs: 30_000,
    autoReconnectLocal: true,
    confirmDangerousWrites: true,
  },
  ai: {
    provider: "openai",
    model: "gpt-5.6",
    reasoning: "medium",
    dataSharing: "schemaOnly",
  },
};

export const defaultConnectorDescriptors: ConnectorDescriptor[] = [
  {
    dataSourceKind: "ordadbNative",
    connectorId: "ordadb-native",
    connectorKind: "sql",
    commandLanguage: "postgresql-sql",
    editorMode: "sql",
    dialect: "postgresql",
    displayName: "OrdaDB",
    defaultEndpoint: "127.0.0.1:54329",
    defaultAdminEndpoint: "http://127.0.0.1:9080",
    defaultDatabase: "ordadb",
    defaultTlsMode: "disable",
    logoAsset: "ordadb",
  },
  {
    dataSourceKind: "postgresql",
    connectorId: "postgresql",
    connectorKind: "sql",
    commandLanguage: "postgresql-sql",
    editorMode: "sql",
    dialect: "postgresql",
    displayName: "PostgreSQL",
    defaultEndpoint: "127.0.0.1:5432",
    defaultDatabase: "postgres",
    defaultTlsMode: "prefer",
    logoAsset: "postgresql",
  },
  {
    dataSourceKind: "mysql",
    connectorId: "mysql",
    connectorKind: "sql",
    commandLanguage: "mysql-sql",
    editorMode: "sql",
    dialect: "mysql",
    displayName: "MySQL",
    defaultEndpoint: "127.0.0.1:3306",
    defaultTlsMode: "prefer",
    logoAsset: "mysql",
  },
  {
    dataSourceKind: "sqlite",
    connectorId: "sqlite",
    connectorKind: "sql",
    commandLanguage: "sqlite-sql",
    editorMode: "sql",
    dialect: "sqlite",
    displayName: "SQLite",
    defaultEndpoint: "",
    defaultTlsMode: "disable",
    logoAsset: "sqlite",
  },
  {
    dataSourceKind: "sqlServer",
    connectorId: "sql-server",
    connectorKind: "sql",
    commandLanguage: "sql-server-sql",
    editorMode: "sql",
    dialect: "sqlServer",
    displayName: "SQL Server",
    defaultEndpoint: "127.0.0.1:1433",
    defaultTlsMode: "require",
    logoAsset: "sql-server",
  },
  {
    dataSourceKind: "mongodb",
    connectorId: "mongodb",
    connectorKind: "document",
    commandLanguage: "mongodb-json",
    editorMode: "json",
    displayName: "MongoDB",
    defaultEndpoint: "127.0.0.1:27017",
    defaultDatabase: "admin",
    defaultTlsMode: "prefer",
    logoAsset: "mongodb",
  },
  {
    dataSourceKind: "redis",
    connectorId: "redis",
    connectorKind: "keyValue",
    commandLanguage: "redis-resp3",
    editorMode: "plaintext",
    displayName: "Redis",
    defaultEndpoint: "127.0.0.1:6379",
    defaultDatabase: "0",
    defaultTlsMode: "disable",
    logoAsset: "redis",
  },
  {
    dataSourceKind: "mariadb",
    connectorId: "mariadb",
    connectorKind: "sql",
    commandLanguage: "mariadb-sql",
    editorMode: "sql",
    dialect: "mariadb",
    displayName: "MariaDB",
    defaultEndpoint: "127.0.0.1:3306",
    defaultTlsMode: "require",
    logoAsset: "mariadb",
  },
  {
    dataSourceKind: "clickhouse",
    connectorId: "clickhouse",
    connectorKind: "sql",
    commandLanguage: "clickhouse-sql",
    editorMode: "sql",
    dialect: "clickhouse",
    displayName: "ClickHouse",
    defaultEndpoint: "127.0.0.1:8123",
    defaultDatabase: "default",
    defaultTlsMode: "disable",
    logoAsset: "clickhouse",
  },
  {
    dataSourceKind: "oracle",
    connectorId: "oracle",
    connectorKind: "sql",
    commandLanguage: "oracle-sql",
    editorMode: "sql",
    dialect: "oracle",
    displayName: "Oracle",
    defaultEndpoint: "127.0.0.1:1521",
    defaultDatabase: "ORCLPDB1",
    defaultTlsMode: "disable",
    logoAsset: "oracle",
  },
];

class TauriConsoleClient implements ConsoleClient {
  readonly mode = "desktop";

  bootstrap() {
    return invoke<ConsoleBootstrap>("console_bootstrap");
  }

  saveSettings(settings: ConsoleSettingsV2) {
    return invoke<ConsoleSettingsV2>("console_save_settings", { settings });
  }

  pickWorkspace() {
    return invoke<WorkspaceSnapshot | null>("workspace_pick_folder");
  }

  pickDocument() {
    return invoke<SqlDocument | null>("workspace_pick_document");
  }

  openWorkspace(rootPath: string) {
    return invoke<WorkspaceSnapshot>("workspace_open", {
      request: { rootPath },
    });
  }

  openDocument(rootPath: string, path: string) {
    return invoke<SqlDocument>("workspace_open_document", {
      request: { rootPath, path },
    });
  }

  openExternalDocument(path: string) {
    return invoke<SqlDocument>("workspace_open_external_document", {
      request: { path },
    });
  }

  newDocument(rootPath: string, parentPath: string, fileName: string) {
    return invoke<SqlDocument>("workspace_new_document", {
      request: { rootPath, parentPath, fileName },
    });
  }

  saveDocument(
    rootPath: string,
    document: OpenSqlDocument,
    force = false,
  ) {
    return invoke<SqlDocument>("workspace_save_document", {
      request: {
        rootPath,
        path: document.path,
        content: document.content,
        expectedRevision: document.revision,
        force,
      },
    });
  }

  saveExternalDocument(document: OpenSqlDocument, force = false) {
    if (document.locator.kind !== "external") {
      return Promise.reject(
        new Error("saveExternalDocument requires an external locator"),
      );
    }
    return invoke<SqlDocument>("workspace_save_external_document", {
      request: {
        path: document.locator.path,
        content: document.content,
        expectedRevision: document.revision,
        force,
      },
    });
  }

  saveDocumentAs(request: SaveDocumentAsRequest) {
    return invoke<SqlDocument | null>("workspace_save_document_as", { request });
  }

  renameEntry(rootPath: string, path: string, newName: string) {
    return invoke<WorkspaceSnapshot>("workspace_rename_entry", {
      request: { rootPath, path, newName },
    });
  }

  trashEntry(rootPath: string, path: string) {
    return invoke<WorkspaceSnapshot>("workspace_trash_entry", {
      request: { rootPath, path },
    });
  }

  saveSession(session: WorkspaceSessionV1) {
    return invoke<void>("workspace_save_session", { session });
  }

  saveConnectionProfile(profile: ConnectionProfileV3) {
    return invoke<ConnectionProfileV3[]>("console_save_connection_profile", {
      profile,
    });
  }

  deleteConnectionProfile(profileId: string) {
    return invoke<ConnectionProfileV3[]>("console_delete_connection_profile", {
      profileId,
    });
  }
}

export class PreviewConsoleClient implements ConsoleClient {
  readonly mode = "preview";
  private settings = cloneConsoleSettings(defaultConsoleSettings);
  private profiles: ConnectionProfileV3[] = [];
  private session: WorkspaceSessionV1 = emptyPreviewSession();
  private revisionSequence = 3;
  private recentFiles: RecentFileEntry[] = [];
  private documents = new Map<string, PreviewDocument>([
    [
      "queries/customers.sql",
      {
        content: "select * from customers;\n",
        modifiedAtMs: 1,
      },
    ],
    [
      "scratch.sql",
      {
        content: "select 1;\n",
        modifiedAtMs: 2,
      },
    ],
  ]);

  bootstrap: ConsoleClient["bootstrap"] = async () => ({
    settings: cloneConsoleSettings(this.settings),
    recovery:
      this.session.openDocuments.length > 0 ? cloneSession(this.session) : null,
    recentFiles: this.recentFiles.map(cloneRecentFile),
    connectionProfiles: this.profiles.map((profile) => ({ ...profile })),
    connectorDescriptors: defaultConnectorDescriptors.map((descriptor) => ({
      ...descriptor,
    })),
  });

  saveSettings: ConsoleClient["saveSettings"] = async (settings) => {
    this.settings = cloneConsoleSettings(settings);
    return cloneConsoleSettings(this.settings);
  };

  pickWorkspace: ConsoleClient["pickWorkspace"] = async () =>
    this.workspaceSnapshot();

  pickDocument: ConsoleClient["pickDocument"] = async () => null;

  openWorkspace: ConsoleClient["openWorkspace"] = async (rootPath) => {
    this.assertPreviewRoot(rootPath);
    return this.workspaceSnapshot();
  };

  openDocument: ConsoleClient["openDocument"] = async (rootPath, path) => {
    this.assertPreviewRoot(rootPath);
    return this.openPreviewDocument(path);
  };

  openExternalDocument: ConsoleClient["openExternalDocument"] = async () => {
    throw previewFileError(
      "Preview 不读取本机外部文件；请在 Windows 桌面版中打开",
      "0A000",
    );
  };

  newDocument: ConsoleClient["newDocument"] = async (
    rootPath,
    parentPath,
    fileName,
  ) => {
    this.assertPreviewRoot(rootPath);
    if (!fileName.toLowerCase().endsWith(".sql")) {
      throw previewFileError("Preview SQL 文件必须使用 .sql 扩展名");
    }
    const path = [parentPath, fileName].filter(Boolean).join("/");
    if (this.documents.has(path)) {
      throw previewFileError("Preview SQL 文件已存在", "42P07");
    }
    this.documents.set(path, {
      content: "",
      modifiedAtMs: this.revisionSequence++,
    });
    return this.openPreviewDocument(path);
  };

  saveDocument: ConsoleClient["saveDocument"] = async (
    rootPath,
    document,
    force = false,
  ) => {
    this.assertPreviewRoot(rootPath);
    const current = await this.sqlDocument(document.path);
    if (!force && !sameFileRevision(current.revision, document.revision)) {
      throw previewFileError("Preview SQL 文件已在外部修改", "40001");
    }
    this.documents.set(document.path, {
      content: document.content,
      modifiedAtMs: this.revisionSequence++,
    });
    return this.sqlDocument(document.path);
  };

  saveExternalDocument: ConsoleClient["saveExternalDocument"] = async () => {
    throw previewFileError(
      "Preview 不写入本机外部文件；请使用桌面版",
      "0A000",
    );
  };

  saveDocumentAs: ConsoleClient["saveDocumentAs"] = async (request) => {
    const fileName = previewUniqueFileName(
      this.documents,
      request.suggestedName,
    );
    this.documents.set(fileName, {
      content: request.content,
      modifiedAtMs: this.revisionSequence++,
    });
    return this.openPreviewDocument(fileName);
  };

  renameEntry: ConsoleClient["renameEntry"] = async (
    rootPath,
    path,
    newName,
  ) => {
    this.assertPreviewRoot(rootPath);
    const parent = path.split("/").slice(0, -1).join("/");
    const nextPath = [parent, newName].filter(Boolean).join("/");
    const affected = [...this.documents.entries()].filter(
      ([candidate]) => candidate === path || candidate.startsWith(`${path}/`),
    );
    if (affected.length === 0) {
      throw previewFileError("Preview 项目条目不存在", "42704");
    }
    for (const [candidate] of affected) {
      const replacement = `${nextPath}${candidate.slice(path.length)}`;
      if (this.documents.has(replacement) && !affected.some(([key]) => key === replacement)) {
        throw previewFileError("Preview 项目条目已存在", "42P07");
      }
    }
    for (const [candidate] of affected) this.documents.delete(candidate);
    for (const [candidate, value] of affected) {
      this.documents.set(`${nextPath}${candidate.slice(path.length)}`, value);
    }
    return this.workspaceSnapshot();
  };

  trashEntry: ConsoleClient["trashEntry"] = async (rootPath, path) => {
    this.assertPreviewRoot(rootPath);
    let removed = false;
    for (const candidate of [...this.documents.keys()]) {
      if (candidate === path || candidate.startsWith(`${path}/`)) {
        this.documents.delete(candidate);
        removed = true;
      }
    }
    if (!removed) throw previewFileError("Preview 项目条目不存在", "42704");
    return this.workspaceSnapshot();
  };

  saveSession: ConsoleClient["saveSession"] = async (session) => {
    this.session = cloneSession(session);
  };

  saveConnectionProfile: ConsoleClient["saveConnectionProfile"] = async (
    profile,
  ) => {
    this.profiles = [
      ...this.profiles.filter(
        (candidate) => candidate.profileId !== profile.profileId,
      ),
      { ...profile },
    ];
    return this.profiles.map((candidate) => ({ ...candidate }));
  };

  deleteConnectionProfile: ConsoleClient["deleteConnectionProfile"] = async (
    profileId,
  ) => {
    this.profiles = this.profiles.filter(
      (candidate) => candidate.profileId !== profileId,
    );
    return this.profiles.map((candidate) => ({ ...candidate }));
  };

  private assertPreviewRoot(rootPath: string) {
    if (rootPath !== PREVIEW_WORKSPACE_ROOT) {
      throw previewFileError("Preview SQL 项目不存在", "42704");
    }
  }

  private workspaceSnapshot(): WorkspaceSnapshot {
    const directories = new Set<string>();
    for (const path of this.documents.keys()) {
      const parts = path.split("/");
      for (let index = 1; index < parts.length; index += 1) {
        directories.add(parts.slice(0, index).join("/"));
      }
    }
    const entries: WorkspaceEntry[] = [
      ...[...directories].map((path) => ({
        path,
        name: path.split("/").at(-1) ?? path,
        kind: "directory" as const,
        depth: path.split("/").length,
      })),
      ...[...this.documents.keys()].map((path) => ({
        path,
        name: path.split("/").at(-1) ?? path,
        kind: "sqlFile" as const,
        depth: path.split("/").length,
      })),
    ].sort((left, right) => left.path.localeCompare(right.path));
    return {
      formatVersion: 1,
      rootPath: PREVIEW_WORKSPACE_ROOT,
      entries,
    };
  }

  private async sqlDocument(path: string): Promise<SqlDocument> {
    const document = this.documents.get(path);
    if (!document) throw previewFileError("Preview SQL 文件不存在", "42704");
    return {
      locator: {
        kind: "workspace",
        rootPath: PREVIEW_WORKSPACE_ROOT,
        path,
      },
      path,
      name: path.split("/").at(-1) ?? path,
      content: document.content,
      revision: await previewRevision(document),
    };
  }

  private async openPreviewDocument(path: string): Promise<SqlDocument> {
    const document = await this.sqlDocument(path);
    this.recentFiles = [
      {
        locator: document.locator,
        name: document.name,
        openedAtMs: Date.now(),
      },
      ...this.recentFiles.filter(
        (entry) =>
          documentLocatorKey(entry.locator) !==
          documentLocatorKey(document.locator),
      ),
    ].slice(0, 50);
    return document;
  }
}

let client: ConsoleClient | undefined;

export function getConsoleClient(): ConsoleClient {
  client ??= isTauriRuntime()
    ? new TauriConsoleClient()
    : new PreviewConsoleClient();
  return client;
}

const PREVIEW_WORKSPACE_ROOT = "Preview fixture";

interface PreviewDocument {
  content: string;
  modifiedAtMs: number;
}

export function cloneConsoleSettings(
  settings: ConsoleSettingsV2,
): ConsoleSettingsV2 {
  return {
    ...settings,
    appearance: { ...settings.appearance },
    editor: { ...settings.editor },
    files: { ...settings.files },
    results: { ...settings.results },
    connections: { ...settings.connections },
    ai: { ...settings.ai },
  };
}

function emptyPreviewSession(): WorkspaceSessionV1 {
  return {
    formatVersion: 1,
    rootPath: null,
    activePath: null,
    openDocuments: [],
  };
}

function cloneSession(session: WorkspaceSessionV1): WorkspaceSessionV1 {
  return {
    ...session,
    openDocuments: session.openDocuments.map((document) => ({
      ...document,
      locator: document.locator ? { ...document.locator } : undefined,
      baseRevision: document.baseRevision
        ? { ...document.baseRevision }
        : null,
    })),
  };
}

function sameFileRevision(left: FileRevision, right: FileRevision | null) {
  if (!right) return false;
  return (
    left.sizeBytes === right.sizeBytes &&
    left.modifiedAtMs === right.modifiedAtMs &&
    left.sha256 === right.sha256
  );
}

function cloneRecentFile(entry: RecentFileEntry): RecentFileEntry {
  return {
    ...entry,
    locator: { ...entry.locator },
  };
}

function documentLocatorKey(locator: DocumentLocator) {
  switch (locator.kind) {
    case "workspace":
      return `workspace:${locator.rootPath.toLocaleLowerCase()}:${locator.path.toLocaleLowerCase()}`;
    case "external":
      return `external:${locator.path.toLocaleLowerCase()}`;
    case "untitled":
      return `untitled:${locator.id}`;
  }
}

function previewUniqueFileName(
  documents: Map<string, PreviewDocument>,
  suggestedName: string,
) {
  const normalized = suggestedName.toLowerCase().endsWith(".sql")
    ? suggestedName
    : `${suggestedName}.sql`;
  if (!documents.has(normalized)) return normalized;
  const stem = normalized.slice(0, -4);
  for (let sequence = 2; sequence <= 10_000; sequence += 1) {
    const candidate = `${stem}-${sequence}.sql`;
    if (!documents.has(candidate)) return candidate;
  }
  throw previewFileError("Preview SQL 文件数量已达到上限", "54000");
}

async function previewRevision(document: PreviewDocument): Promise<FileRevision> {
  const bytes = new TextEncoder().encode(document.content);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return {
    sizeBytes: bytes.byteLength,
    modifiedAtMs: document.modifiedAtMs,
    sha256: [...new Uint8Array(digest)]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join(""),
  };
}

function previewFileError(message = "浏览器 Preview 不访问本地 SQL 项目", sqlState = "0A000") {
  return {
    sqlState,
    message,
    detail: null,
    hint: "Preview 项目只保存在当前页面内，不访问或写入本地文件",
    position: null,
    queryId: "preview-workspace",
  };
}
