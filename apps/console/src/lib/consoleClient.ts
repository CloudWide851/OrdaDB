import { invoke } from "@tauri-apps/api/core";
import type { SqlDialect } from "../types";
import { isTauriRuntime } from "./tauri";

export interface ConsoleSettingsV1 {
  formatVersion: 1;
  uiFontSize: number;
  dataFontSize: number;
  editorFontSize: number;
  density: "compact";
  reopenLastProject: boolean;
  hideEmptyCatalog: boolean;
}

export interface FileRevision {
  sizeBytes: number;
  modifiedAtMs: number;
  sha256: string;
}

export interface SqlDocument {
  path: string;
  name: string;
  content: string;
  revision: FileRevision;
}

export interface OpenSqlDocument extends SqlDocument {
  savedContent: string;
  dirty: boolean;
  conflict: boolean;
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
  content: string;
  baseRevision: FileRevision | null;
}

export interface WorkspaceSessionV1 {
  formatVersion: 1;
  rootPath: string | null;
  activePath: string | null;
  openDocuments: WorkspaceDraft[];
}

export interface ConnectionProfileV1 {
  formatVersion: 1;
  profileId: string;
  label: string;
  connectorId: string;
  dialect: SqlDialect;
  endpoint: string;
  adminEndpoint?: string;
  database?: string;
  credentialId: string;
  autoReconnect: boolean;
}

export interface ConsoleBootstrap {
  settings: ConsoleSettingsV1;
  recovery: WorkspaceSessionV1 | null;
  connectionProfiles: ConnectionProfileV1[];
}

export interface ConsoleClient {
  readonly mode: "desktop" | "preview";
  bootstrap(): Promise<ConsoleBootstrap>;
  saveSettings(settings: ConsoleSettingsV1): Promise<ConsoleSettingsV1>;
  pickWorkspace(): Promise<WorkspaceSnapshot | null>;
  openWorkspace(rootPath: string): Promise<WorkspaceSnapshot>;
  openDocument(rootPath: string, path: string): Promise<SqlDocument>;
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
  renameEntry(
    rootPath: string,
    path: string,
    newName: string,
  ): Promise<WorkspaceSnapshot>;
  trashEntry(rootPath: string, path: string): Promise<WorkspaceSnapshot>;
  saveSession(session: WorkspaceSessionV1): Promise<void>;
  saveConnectionProfile(
    profile: ConnectionProfileV1,
  ): Promise<ConnectionProfileV1[]>;
  deleteConnectionProfile(profileId: string): Promise<ConnectionProfileV1[]>;
}

export const defaultConsoleSettings: ConsoleSettingsV1 = {
  formatVersion: 1,
  uiFontSize: 11,
  dataFontSize: 12,
  editorFontSize: 12,
  density: "compact",
  reopenLastProject: false,
  hideEmptyCatalog: true,
};

class TauriConsoleClient implements ConsoleClient {
  readonly mode = "desktop";

  bootstrap() {
    return invoke<ConsoleBootstrap>("console_bootstrap");
  }

  saveSettings(settings: ConsoleSettingsV1) {
    return invoke<ConsoleSettingsV1>("console_save_settings", { settings });
  }

  pickWorkspace() {
    return invoke<WorkspaceSnapshot | null>("workspace_pick_folder");
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

  saveConnectionProfile(profile: ConnectionProfileV1) {
    return invoke<ConnectionProfileV1[]>("console_save_connection_profile", {
      profile,
    });
  }

  deleteConnectionProfile(profileId: string) {
    return invoke<ConnectionProfileV1[]>("console_delete_connection_profile", {
      profileId,
    });
  }
}

export class PreviewConsoleClient implements ConsoleClient {
  readonly mode = "preview";
  private settings = { ...defaultConsoleSettings };
  private profiles: ConnectionProfileV1[] = [];
  private session: WorkspaceSessionV1 = emptyPreviewSession();
  private revisionSequence = 3;
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
    settings: { ...this.settings },
    recovery:
      this.session.rootPath && this.session.openDocuments.length > 0
        ? cloneSession(this.session)
        : null,
    connectionProfiles: this.profiles.map((profile) => ({ ...profile })),
  });

  saveSettings: ConsoleClient["saveSettings"] = async (settings) => {
    this.settings = { ...settings };
    return { ...this.settings };
  };

  pickWorkspace: ConsoleClient["pickWorkspace"] = async () =>
    this.workspaceSnapshot();

  openWorkspace: ConsoleClient["openWorkspace"] = async (rootPath) => {
    this.assertPreviewRoot(rootPath);
    return this.workspaceSnapshot();
  };

  openDocument: ConsoleClient["openDocument"] = async (rootPath, path) => {
    this.assertPreviewRoot(rootPath);
    return this.sqlDocument(path);
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
    return this.sqlDocument(path);
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
      path,
      name: path.split("/").at(-1) ?? path,
      content: document.content,
      revision: await previewRevision(document),
    };
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
      baseRevision: document.baseRevision
        ? { ...document.baseRevision }
        : null,
    })),
  };
}

function sameFileRevision(left: FileRevision, right: FileRevision) {
  return (
    left.sizeBytes === right.sizeBytes &&
    left.modifiedAtMs === right.modifiedAtMs &&
    left.sha256 === right.sha256
  );
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
