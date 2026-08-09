import { create } from "zustand";
import { formatSqlForDialect, getSqlDialect } from "../data/dialects";
import {
  cloneConsoleSettings,
  defaultConnectorDescriptors,
  defaultConsoleSettings,
  getConsoleClient,
  type ConnectionProfileV3,
  type ConnectorDescriptor,
  type ConsoleClient,
  type ConsoleSettingsV2,
  type DocumentLocator,
  type OpenSqlDocument,
  type RecentFileEntry,
  type SqlDocument,
  type WorkspaceSessionV1,
  type WorkspaceSnapshot,
} from "../lib/consoleClient";
import {
  getDbmsClient,
  normalizeDbmsError,
  type ConnectionProbe,
  type DbmsCatalogObject,
  type DbmsClient,
  type DbmsCommand,
  type DbmsConnectionRequest,
  type DbmsConnectionSnapshot,
  type DbmsError,
  type DbmsKeyValue,
  type DbmsMonitorSnapshot,
  type DbmsOperationRecord,
  type DbmsQueryColumn,
  type DbmsServiceStatus,
  type StartDbmsOperationRequest,
} from "../lib/dbmsClient";
import {
  appendResultRows,
  emptyResultBuffer,
  type ResultBuffer,
  type ResultBufferLimits,
} from "../lib/resultBuffer";
import type {
  InspectorTab,
  QueryState,
  ResultTab,
  SqlDialect,
} from "../types";

export type OperationView =
  | "sessions"
  | "locks"
  | "transactions"
  | "roles"
  | "wal"
  | "backup"
  | "importExport"
  | "service";
export type SidebarView = "workspace" | "database";
export type QuickOpenMode = "recent" | "files" | "global";

export interface DataSourceValues extends DbmsConnectionRequest {
  username: string;
}

interface RunQueryOptions {
  sql?: string;
  resultTab?: ResultTab;
}

export interface WorkbenchState {
  runtimeMode: DbmsClient["mode"];
  sql: string;
  settings: ConsoleSettingsV2;
  settingsOpen: boolean;
  workspace: WorkspaceSnapshot | null;
  documents: OpenSqlDocument[];
  activeDocumentPath: string | null;
  recovery: WorkspaceSessionV1 | null;
  recentFiles: RecentFileEntry[];
  connectionProfiles: ConnectionProfileV3[];
  connectorDescriptors: ConnectorDescriptor[];
  connectionProbe: ConnectionProbe | null;
  dialect: SqlDialect;
  sidebarView: SidebarView;
  quickOpenMode: QuickOpenMode | null;
  schemaVisible: boolean;
  inspectorVisible: boolean;
  activeResultTab: ResultTab;
  activeInspectorTab: InspectorTab;
  selectedObject: string;
  selectedCatalogObject: DbmsCatalogObject | null;
  commandPaletteOpen: boolean;
  pluginManagerOpen: boolean;
  dataSourceOpen: boolean;
  operationsOpen: boolean;
  operationView: OperationView;
  notice: string;
  queryState: QueryState;
  columns: DbmsQueryColumn[];
  resultBuffer: ResultBuffer;
  documentResults: unknown[];
  keyValueResults: DbmsKeyValue[];
  structuredResultBytes: number;
  droppedStructuredItems: number;
  logs: string[];
  error: DbmsError | null;
  errorMessage: string | null;
  durationMs: number | null;
  rowsProcessed: number;
  activeRequestId: string | null;
  connection: DbmsConnectionSnapshot | null;
  activeCredentialId: string | null;
  catalog: DbmsCatalogObject[];
  monitor: DbmsMonitorSnapshot | null;
  operations: DbmsOperationRecord[];
  serviceStatus: DbmsServiceStatus | null;
  administrationBusy: boolean;
  connectionState: "idle" | "connecting" | "connected" | "error";
  connectionError: DbmsError | null;
  transactionActive: boolean;
  initialize: () => Promise<void>;
  setSql: (sql: string) => void;
  setSettingsOpen: (open: boolean) => void;
  saveSettings: (settings: ConsoleSettingsV2) => Promise<void>;
  openWorkspace: () => Promise<void>;
  openWorkspacePath: (rootPath: string) => Promise<void>;
  openFile: () => Promise<void>;
  openExternalFiles: (paths: string[]) => Promise<void>;
  openRecentFile: (entry: RecentFileEntry) => Promise<void>;
  restoreRecovery: () => Promise<void>;
  discardRecovery: () => Promise<void>;
  openDocument: (path: string) => Promise<void>;
  createDocument: (parentPath?: string) => Promise<void>;
  activateDocument: (path: string) => void;
  closeDocument: (path: string) => Promise<void>;
  reloadActiveDocument: () => Promise<void>;
  saveActiveDocument: (force?: boolean) => Promise<void>;
  saveActiveDocumentAs: () => Promise<void>;
  saveAllDocuments: () => Promise<void>;
  saveActiveDocumentOnFocusChange: () => Promise<void>;
  formatActiveDocument: () => void;
  renameWorkspaceEntry: (path: string, newName: string) => Promise<void>;
  trashWorkspaceEntry: (path: string) => Promise<void>;
  setDialect: (dialect: SqlDialect) => void;
  setSidebarView: (view: SidebarView) => void;
  setQuickOpenMode: (mode: QuickOpenMode | null) => void;
  setSchemaVisible: (visible: boolean) => void;
  setInspectorVisible: (visible: boolean) => void;
  toggleSchema: () => void;
  toggleInspector: () => void;
  setActiveResultTab: (tab: ResultTab) => void;
  setActiveInspectorTab: (tab: InspectorTab) => void;
  setSelectedObject: (objectName: string) => void;
  setCommandPaletteOpen: (open: boolean) => void;
  setPluginManagerOpen: (open: boolean) => void;
  setDataSourceOpen: (open: boolean) => void;
  openOperations: (view: OperationView) => Promise<void>;
  setOperationsOpen: (open: boolean) => void;
  setNotice: (notice: string) => void;
  bootstrapAdministrator: (values: DataSourceValues) => Promise<void>;
  connectDataSource: (values: DataSourceValues) => Promise<void>;
  disconnectDataSource: () => Promise<void>;
  deleteStoredCredential: () => Promise<void>;
  refreshCatalog: () => Promise<void>;
  refreshMonitor: () => Promise<void>;
  refreshAdministration: () => Promise<void>;
  startAdministrationOperation: (
    request: Omit<StartDbmsOperationRequest, "connectionId">,
  ) => Promise<void>;
  cancelAdministrationOperation: (operationId: string) => Promise<void>;
  runQuery: (options?: RunQueryOptions) => Promise<void>;
  runExplain: () => Promise<void>;
  cancelQuery: () => Promise<void>;
  beginTransaction: () => Promise<void>;
  commitTransaction: () => Promise<void>;
  rollbackTransaction: () => Promise<void>;
  checkpoint: () => Promise<void>;
}

export function createWorkbenchStore(
  dbms: DbmsClient = getDbmsClient(),
  consoleClient: ConsoleClient = getConsoleClient(),
) {
  const sessionSaveController: SessionSaveController = {};
  return create<WorkbenchState>((set, get) => ({
  runtimeMode: dbms.mode,
  sql: "",
  settings: cloneConsoleSettings(defaultConsoleSettings),
  settingsOpen: false,
  workspace: null,
  documents: [],
  activeDocumentPath: null,
  recovery: null,
  recentFiles: [],
  connectionProfiles: [],
  connectorDescriptors: defaultConnectorDescriptors.map((descriptor) => ({
    ...descriptor,
  })),
  connectionProbe: null,
  dialect: "postgresql",
  sidebarView: "workspace",
  quickOpenMode: null,
  schemaVisible: true,
  inspectorVisible: true,
  activeResultTab: "data",
  activeInspectorTab: "properties",
  selectedObject: "",
  selectedCatalogObject: null,
  commandPaletteOpen: false,
  pluginManagerOpen: false,
  dataSourceOpen: false,
  operationsOpen: false,
  operationView: "sessions",
  notice: dbms.mode === "preview" ? "Preview · 未连接" : "未连接",
  queryState: "idle",
  columns: [],
  resultBuffer: emptyResultBuffer(),
  documentResults: [],
  keyValueResults: [],
  structuredResultBytes: 0,
  droppedStructuredItems: 0,
  logs: [],
  error: null,
  errorMessage: null,
  durationMs: null,
  rowsProcessed: 0,
  activeRequestId: null,
  connection: null,
  activeCredentialId: null,
  catalog: [],
  monitor: null,
  operations: [],
  serviceStatus: null,
  administrationBusy: false,
  connectionState: "idle",
  connectionError: null,
  transactionActive: false,

  initialize: async () => {
    try {
      const bootstrap = await consoleClient.bootstrap();
      set({
        settings: bootstrap.settings,
        recovery: bootstrap.recovery,
        recentFiles: bootstrap.recentFiles,
        connectionProfiles: bootstrap.connectionProfiles,
        connectorDescriptors: bootstrap.connectorDescriptors,
      });
      applyConsoleSettings(bootstrap.settings);
      if (
        bootstrap.recovery &&
        (bootstrap.settings.files.recoveryPolicy === "automatic" ||
          bootstrap.settings.files.reopenLastProject)
      ) {
        await get().restoreRecovery();
      } else if (
        bootstrap.recovery &&
        bootstrap.settings.files.recoveryPolicy === "never"
      ) {
        set({ recovery: null });
        await consoleClient.saveSession(emptyWorkspaceSession());
      }
      const reconnect = bootstrap.connectionProfiles.find(
        (profile) =>
          profile.connectorId === "ordadb-native" && profile.autoReconnect,
      );
      if (
        reconnect &&
        bootstrap.settings.connections.autoReconnectLocal &&
        dbms.mode === "desktop"
      ) {
        await connectProfile(reconnect, dbms, consoleClient, set, get);
      }
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        notice: normalized.message,
      });
    }
  },

  setSql: (sql) => {
    set((state) => ({
      sql,
      documents: state.documents.map((document) =>
        document.path === state.activeDocumentPath
          ? {
              ...document,
              content: sql,
              dirty:
                document.locator.kind === "untitled" ||
                sql !== document.savedContent,
            }
          : document,
      ),
    }));
    scheduleSessionSave(sessionSaveController, consoleClient, get);
    scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
  },
  setSettingsOpen: (settingsOpen) => set({ settingsOpen }),
  saveSettings: async (settings) => {
    try {
      const saved = await consoleClient.saveSettings(settings);
      applyConsoleSettings(saved);
      set({
        settings: saved,
        settingsOpen: false,
        notice: "设置已保存",
      });
      scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  openWorkspace: async () => {
    try {
      const snapshot = await consoleClient.pickWorkspace();
      if (!snapshot) return;
      await activateWorkspace(snapshot, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  openWorkspacePath: async (rootPath) => {
    try {
      const snapshot = await consoleClient.openWorkspace(rootPath);
      await activateWorkspace(snapshot, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  openFile: async () => {
    try {
      const document = await consoleClient.pickDocument();
      if (!document) return;
      activateSqlDocument(document, set, get, "已打开");
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  openExternalFiles: async (paths) => {
    for (const path of paths) {
      if (!path.toLowerCase().endsWith(".sql")) continue;
      try {
        const document = await consoleClient.openExternalDocument(path);
        activateSqlDocument(document, set, get, "已打开");
      } catch (error) {
        const normalized = normalizeDbmsError(error);
        set({ notice: normalized.message });
      }
    }
    await persistSession(consoleClient, get);
  },
  openRecentFile: async (entry) => {
    try {
      if (entry.locator.kind === "workspace") {
        if (get().workspace?.rootPath !== entry.locator.rootPath) {
          const snapshot = await consoleClient.openWorkspace(
            entry.locator.rootPath,
          );
          await activateWorkspace(snapshot, consoleClient, set, get);
        }
        await get().openDocument(entry.locator.path);
      } else {
        const document = await consoleClient.openExternalDocument(
          entry.locator.path,
        );
        activateSqlDocument(document, set, get, "已打开");
        await persistSession(consoleClient, get);
      }
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  restoreRecovery: async () => {
    const recovery = get().recovery;
    if (!recovery) return;
    try {
      const snapshot = recovery.rootPath
        ? await consoleClient.openWorkspace(recovery.rootPath)
        : null;
      const documents: OpenSqlDocument[] = [];
      for (const draft of recovery.openDocuments) {
        const locator =
          draft.locator ??
          (recovery.rootPath
            ? {
                kind: "workspace" as const,
                rootPath: recovery.rootPath,
                path: draft.path,
              }
            : null);
        if (!locator) continue;
        if (locator.kind === "untitled") {
          documents.push({
            locator,
            path: draft.path,
            name: draft.name ?? nextUntitledName(documents),
            content: draft.content,
            revision: null,
            savedContent: "",
            dirty: true,
            conflict: false,
          });
          continue;
        }
        const current =
          locator.kind === "workspace"
            ? await consoleClient.openDocument(locator.rootPath, locator.path)
            : await consoleClient.openExternalDocument(locator.path);
        documents.push({
          ...current,
          content: draft.content,
          savedContent: current.content,
          dirty: draft.content !== current.content,
          conflict:
            draft.baseRevision !== null &&
            !sameRevision(draft.baseRevision, current.revision),
        });
      }
      const activePath =
        recovery.activePath &&
        documents.some((document) => document.path === recovery.activePath)
          ? recovery.activePath
          : documents[0]?.path ?? null;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath: activePath,
        sql:
          documents.find((document) => document.path === activePath)?.content ??
          "",
        recovery: null,
        notice: `已恢复 ${documents.length} 个 SQL 草稿`,
      });
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  discardRecovery: async () => {
    set({ recovery: null });
    await consoleClient.saveSession(emptyWorkspaceSession());
    set({ notice: "已丢弃上次草稿" });
  },
  openDocument: async (path) => {
    const workspace = get().workspace;
    if (!workspace) return;
    const existing = get().documents.find((document) => document.path === path);
    if (existing) {
      get().activateDocument(path);
      return;
    }
    try {
      const document = await consoleClient.openDocument(workspace.rootPath, path);
      activateSqlDocument(document, set, get, "已打开");
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  createDocument: async (parentPath = "") => {
    void parentPath;
    const id = nextUntitledId(get().documents);
    const name = nextUntitledName(get().documents);
    const document: OpenSqlDocument = {
      locator: { kind: "untitled", id },
      path: `untitled:${id}`,
      name,
      content: "",
      revision: null,
      savedContent: "",
      dirty: true,
      conflict: false,
    };
    set((state) => ({
      documents: [...state.documents, document],
      activeDocumentPath: document.path,
      sql: "",
      notice: `${name} · 首次保存时选择位置`,
    }));
    scheduleSessionSave(sessionSaveController, consoleClient, get);
    scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
  },
  activateDocument: (path) => {
    const document = get().documents.find((candidate) => candidate.path === path);
    if (!document) return;
    set({
      activeDocumentPath: path,
      sql: document.content,
      notice: document.dirty ? `${document.name} · 未保存` : document.name,
    });
    scheduleSessionSave(sessionSaveController, consoleClient, get);
  },
  closeDocument: async (path) => {
    const documents = get().documents.filter(
      (document) => document.path !== path,
    );
    const activePath =
      get().activeDocumentPath === path
        ? documents.at(-1)?.path ?? null
        : get().activeDocumentPath;
    set({
      documents,
      activeDocumentPath: activePath,
      sql:
        documents.find((document) => document.path === activePath)?.content ?? "",
      notice: "SQL 文件已关闭",
    });
    await persistSession(consoleClient, get);
  },
  reloadActiveDocument: async () => {
    const activePath = get().activeDocumentPath;
    const active = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!active || active.locator.kind === "untitled") return;
    try {
      const document =
        active.locator.kind === "workspace"
          ? await consoleClient.openDocument(
              active.locator.rootPath,
              active.locator.path,
            )
          : await consoleClient.openExternalDocument(active.locator.path);
      const reloaded = toOpenDocument(document);
      set((state) => ({
        documents: state.documents.map((candidate) =>
          candidate.path === activePath ? reloaded : candidate,
        ),
        sql: reloaded.content,
        notice: `${reloaded.name} · 已从磁盘重新加载`,
      }));
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  saveActiveDocument: async (force = false) => {
    const activePath = get().activeDocumentPath;
    const document = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!document) return;
    if (document.locator.kind === "untitled") {
      await get().saveActiveDocumentAs();
      return;
    }
    clearDocumentAutoSave(sessionSaveController);
    await saveNamedDocument(document.path, force, consoleClient, set, get);
  },
  saveActiveDocumentAs: async () => {
    const activePath = get().activeDocumentPath;
    const document = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!document) return;
    clearDocumentAutoSave(sessionSaveController);
    const prepared = prepareDocumentForSave(
      document,
      get().settings,
      get().dialect,
      get().connection?.connectorKind !== "document" &&
        get().connection?.connectorKind !== "keyValue",
    );
    try {
      const saved = await consoleClient.saveDocumentAs({
        content: prepared.content,
        suggestedName:
          document.locator.kind === "untitled" ? document.name : document.name,
      });
      if (!saved) {
        set({ notice: "已取消保存" });
        return;
      }
      const open = toOpenDocument(saved);
      set((state) => ({
        documents: state.documents.map((candidate) =>
          candidate.path === activePath ? open : candidate,
        ),
        activeDocumentPath: open.path,
        sql: open.content,
        recentFiles: addRecentFile(state.recentFiles, saved),
        notice: `${open.name} · 已另存为`,
      }));
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  saveAllDocuments: async () => {
    clearDocumentAutoSave(sessionSaveController);
    for (const document of get().documents.filter((candidate) => candidate.dirty)) {
      const prepared = prepareDocumentForSave(
        document,
        get().settings,
        get().dialect,
        get().connection?.connectorKind !== "document" &&
          get().connection?.connectorKind !== "keyValue",
      );
      try {
        const saved =
          prepared.locator.kind === "untitled"
            ? await consoleClient.saveDocumentAs({
                content: prepared.content,
                suggestedName: prepared.name,
              })
            : await saveOpenDocument(consoleClient, prepared);
        if (!saved) return;
        set((state) => ({
          documents: state.documents.map((candidate) =>
            candidate.path === document.path
              ? toOpenDocument(saved)
              : candidate,
          ),
          activeDocumentPath:
            state.activeDocumentPath === document.path
              ? saved.path
              : state.activeDocumentPath,
          recentFiles: addRecentFile(state.recentFiles, saved),
        }));
      } catch (error) {
        const normalized = normalizeDbmsError(error);
        set((state) => ({
          documents: state.documents.map((candidate) =>
            candidate.path === document.path
              ? { ...candidate, conflict: normalized.sqlState === "40001" }
              : candidate,
          ),
          notice: normalized.message,
        }));
        return;
      }
    }
    const active = get().documents.find(
      (document) => document.path === get().activeDocumentPath,
    );
    set({ sql: active?.content ?? "", notice: "全部 SQL 文件已保存" });
    await persistSession(consoleClient, get);
  },
  saveActiveDocumentOnFocusChange: async () => {
    const state = get();
    const document = state.documents.find(
      (candidate) => candidate.path === state.activeDocumentPath,
    );
    if (
      state.settings.files.autoSave !== "onFocusChange" ||
      !document?.dirty ||
      document.conflict ||
      document.locator.kind === "untitled"
    ) {
      return;
    }
    await state.saveActiveDocument();
  },
  formatActiveDocument: () => {
    const state = get();
    if (!state.activeDocumentPath) return;
    if (
      state.connection?.connectorKind === "document" ||
      state.connection?.connectorKind === "keyValue"
    ) {
      set({ notice: "当前命令语言不使用 SQL 格式化器" });
      return;
    }
    const dialect = getSqlDialect(state.dialect);
    state.setSql(formatSqlForDialect(state.sql, dialect));
    set({ notice: `格式化 SQL · ${dialect.label}` });
  },
  renameWorkspaceEntry: async (path, newName) => {
    const workspace = get().workspace;
    if (!workspace) return;
    try {
      const snapshot = await consoleClient.renameEntry(
        workspace.rootPath,
        path,
        newName,
      );
      const nextPath = renamedPath(path, newName);
      const documents = get().documents.map((document) => {
        if (
          document.locator.kind !== "workspace" ||
          document.locator.rootPath !== workspace.rootPath
        ) {
          return document;
        }
        const pathAfterRename = replacePathPrefix(
          document.locator.path,
          path,
          nextPath,
        );
        return {
          ...document,
          path: pathAfterRename,
          name: pathAfterRename.split("/").at(-1) ?? document.name,
          locator: {
            ...document.locator,
            path: pathAfterRename,
          },
        };
      });
      const activeIndex = get().documents.findIndex(
        (document) => document.path === get().activeDocumentPath,
      );
      const activeDocumentPath =
        activeIndex >= 0
          ? documents[activeIndex]?.path ?? null
          : get().activeDocumentPath;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath,
        notice: "项目条目已重命名",
      });
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  trashWorkspaceEntry: async (path) => {
    const workspace = get().workspace;
    if (!workspace) return;
    try {
      const snapshot = await consoleClient.trashEntry(workspace.rootPath, path);
      const documents = get().documents.filter(
        (document) =>
          document.locator.kind !== "workspace" ||
          document.locator.rootPath !== workspace.rootPath ||
          (document.locator.path !== path &&
            !document.locator.path.startsWith(`${path}/`)),
      );
      const activeDocumentPath = documents.some(
        (document) => document.path === get().activeDocumentPath,
      )
        ? get().activeDocumentPath
        : documents.at(-1)?.path ?? null;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath,
        sql:
          documents.find(
            (document) => document.path === activeDocumentPath,
          )?.content ?? "",
        notice: "项目条目已移入回收站",
      });
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  setDialect: (dialect) =>
    set({
      dialect,
      notice: `SQL 方言 · ${getSqlDialect(dialect).label}${
        get().connection?.mode === "preview" ? " · Preview" : ""
      }`,
    }),
  setSidebarView: (sidebarView) => set({ sidebarView }),
  setQuickOpenMode: (quickOpenMode) => set({ quickOpenMode }),
  setSchemaVisible: (schemaVisible) => set({ schemaVisible }),
  setInspectorVisible: (inspectorVisible) => set({ inspectorVisible }),
  toggleSchema: () => set((state) => ({ schemaVisible: !state.schemaVisible })),
  toggleInspector: () =>
    set((state) => ({ inspectorVisible: !state.inspectorVisible })),
  setActiveResultTab: (activeResultTab) => set({ activeResultTab }),
  setActiveInspectorTab: (activeInspectorTab) => set({ activeInspectorTab }),
  setSelectedObject: (identifier) => {
    const selected =
      get().catalog.find((object) => object.id === identifier) ??
      get().catalog.find((object) => object.name === identifier) ??
      null;
    set({
      selectedObject: selected ? catalogObjectIdentity(selected) : "",
      selectedCatalogObject: selected,
    });
  },
  setCommandPaletteOpen: (commandPaletteOpen) => set({ commandPaletteOpen }),
  setPluginManagerOpen: (pluginManagerOpen) => set({ pluginManagerOpen }),
  setDataSourceOpen: (dataSourceOpen) => set({ dataSourceOpen }),
  setOperationsOpen: (operationsOpen) => set({ operationsOpen }),
  setNotice: (notice) => set({ notice }),

  bootstrapAdministrator: async (values) => {
    try {
      const request = toConnectionRequest(values);
      const ticket = get().connectionProbe?.bootstrapTicket;
      if (values.connectorId !== "ordadb-native") {
        throw localError(
          "0A000",
          "管理员初始化仅适用于本机 OrdaDB 数据源",
        );
      }
      if (!ticket) {
        throw localError(
          "55000",
          "本地初始化票据不存在或已失效，请重新运行连接诊断",
        );
      }
      const result = await dbms.bootstrapAdmin({
        ticket: ticket.ticket,
        connection: request,
        suggestedUsername: values.username,
      });
      if (!result.success || result.error) {
        throw result.error ?? localError("55000", "管理员初始化未完成");
      }
      set({
        activeCredentialId: values.credentialId,
        notice: `${result.user ?? values.username} · 管理员初始化完成，正在复检`,
      });
      const probe = await runConnectionStep(
        "管理员初始化复检",
        dbms.probe(request),
        get,
      );
      set({ connectionProbe: probe });
      requireReadyProbe(probe, "管理员初始化后的连接诊断未通过");
      await establishDataSourceConnection(
        values,
        request,
        dbms,
        consoleClient,
        set,
        get,
      );
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        connectionState: "error",
        connectionError: normalized,
        notice: normalized.message,
      });
      throw normalized;
    }
  },

  openOperations: async (operationView) => {
    set({ operationView, operationsOpen: true });
    await Promise.all([get().refreshMonitor(), get().refreshAdministration()]);
  },

  connectDataSource: async (values) => {
    set({
      connectionState: "connecting",
      connectionError: null,
      notice: "正在连接数据源",
    });
    try {
      const request = toConnectionRequest(values);
      if (values.connectorId === "ordadb-native") {
        const preflight = await runConnectionStep(
          "本地服务诊断",
          dbms.probe(request),
          get,
        );
        set({ connectionProbe: preflight });
        if (preflight.bootstrapTicket) {
          throw (
            failedProbeError(preflight) ??
            localError("55000", "OrdaDB 需要创建首位管理员")
          );
        }
        requireNativePreflight(preflight);
      }
      const credential = await dbms.promptCredential({
        credentialId: values.credentialId,
        connectorId: values.connectorId,
        suggestedUsername: values.username,
      });
      if (!credential) {
        throw localError("57014", "已取消 Windows 凭据输入");
      }
      set({ activeCredentialId: values.credentialId });
      const probe = await runConnectionStep(
        "连接诊断",
        dbms.probe(request),
        get,
      );
      set({ connectionProbe: probe });
      requireReadyProbe(probe, "数据库连接诊断未通过");
      await establishDataSourceConnection(
        values,
        request,
        dbms,
        consoleClient,
        set,
        get,
      );
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        connectionState: "error",
        connectionError: normalized,
        notice: `连接失败 · ${normalized.sqlState}`,
      });
      throw normalized;
    }
  },

  disconnectDataSource: async () => {
    const connection = get().connection;
    if (!connection || connection.mode === "preview") return;
    try {
      await dbms.disconnect(connection.connectionId);
      set({
        connection: null,
        catalog: [],
        monitor: null,
        operations: [],
        serviceStatus: null,
        selectedCatalogObject: null,
        connectionState: "idle",
        transactionActive: false,
        notice: "数据源已断开",
      });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  deleteStoredCredential: async () => {
    const credentialId = get().activeCredentialId;
    if (!credentialId) return;
    if (get().connection?.mode !== "preview") {
      await get().disconnectDataSource();
    }
    try {
      await dbms.deleteCredential(credentialId);
      const profiles =
        await consoleClient.deleteConnectionProfile(credentialId);
      set({
        activeCredentialId: null,
        connectionProfiles: profiles,
        notice: "已删除 Windows 凭据",
      });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  refreshCatalog: async () => {
    const connection = get().connection;
    if (!connection) {
      set({ dataSourceOpen: true, notice: "请先连接数据源" });
      return;
    }
    if (!connection.capabilities.catalog) {
      set({ catalog: [], notice: "当前数据源不提供 Catalog" });
      return;
    }
    try {
      const catalog = await dbms.catalog(connection.connectionId);
      setCatalog(set, get, catalog.objects);
      set({ notice: `对象树已刷新 · ${catalog.objects.length} 个对象` });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  refreshMonitor: async () => {
    const connection = get().connection;
    if (!connection) return;
    if (!supportsMonitor(connection)) {
      set({ monitor: null });
      return;
    }
    try {
      const monitor = await dbms.monitor(connection.connectionId);
      set({ monitor });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  refreshAdministration: async () => {
    const connection = get().connection;
    if (!connection || connection.mode === "plugin") {
      set({ operations: [], serviceStatus: null });
      return;
    }
    try {
      const [operations, serviceStatus] = await Promise.all([
        dbms.operations(connection.connectionId),
        dbms.service(connection.connectionId),
      ]);
      set({ operations, serviceStatus, connectionError: null });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  startAdministrationOperation: async (request) => {
    const connection = get().connection;
    if (!connection) {
      set({ dataSourceOpen: true, notice: "请先连接数据源" });
      return;
    }
    set({ administrationBusy: true, connectionError: null });
    try {
      const operation = await dbms.startOperation({
        ...request,
        connectionId: connection.connectionId,
      });
      set((state) => ({
        operations: replaceOperation(state.operations, operation),
        administrationBusy: false,
        notice: `${operationLabel(operation.kind)}已排队`,
      }));
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        administrationBusy: false,
        connectionError: normalized,
        notice: normalized.message,
      });
    }
  },

  cancelAdministrationOperation: async (operationId) => {
    const connection = get().connection;
    if (!connection) return;
    try {
      const operation = await dbms.cancelOperation(
        connection.connectionId,
        operationId,
      );
      set((state) => ({
        operations: replaceOperation(state.operations, operation),
        notice: "已发送作业取消请求",
      }));
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },

  runQuery: async (options = {}) => {
    const input = (options.sql ?? get().sql).trim();
    const connection = get().connection;
    if (!input) {
      const error = localError("42601", "命令不能为空");
      setQueryError(set, error);
      return;
    }
    if (!connection) {
      set({
        dataSourceOpen: true,
        notice: "请先连接数据源",
      });
      return;
    }
    if (
      get().settings.connections.confirmDangerousWrites &&
      connection.connectorKind === "sql" &&
      requiresDangerousWriteConfirmation(input) &&
      (typeof window === "undefined" ||
        !window.confirm("该语句可能修改数据库。确认继续执行吗？"))
    ) {
      set({ notice: "已取消可能修改数据库的语句" });
      return;
    }
    const queryTimeoutMs = get().settings.results.queryTimeoutMs;
    const queryDeadline = Date.now() + queryTimeoutMs;
    const resultLimits = resultBufferLimits(get().settings);
    let command: DbmsCommand;
    try {
      command = buildDbmsCommand(connection, input);
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
      return;
    }
    set({
      queryState: "running",
      columns: [],
      resultBuffer: emptyResultBuffer(),
      documentResults: [],
      keyValueResults: [],
      structuredResultBytes: 0,
      droppedStructuredItems: 0,
      logs: [],
      error: null,
      errorMessage: null,
      durationMs: null,
      rowsProcessed: 0,
      activeRequestId: null,
      activeResultTab: options.resultTab ?? "data",
      notice:
        connection.mode === "preview" ? "正在运行 Preview 命令" : "正在运行命令",
    });
    try {
      const operation = await withTimeout(
        dbms.execute(connection.connectionId, command),
        queryTimeoutMs,
        () => queryTimeoutError(queryTimeoutMs),
      );
      set({ activeRequestId: operation.requestId });
      let terminal = false;
      const iterator = operation.events[Symbol.asyncIterator]();
      while (true) {
        const remainingMs = Math.max(1, queryDeadline - Date.now());
        let next: IteratorResult<
          Awaited<ReturnType<typeof iterator.next>>["value"]
        >;
        try {
          next = await withTimeout(iterator.next(), remainingMs, () =>
            queryTimeoutError(queryTimeoutMs),
          );
        } catch (error) {
          const normalized = normalizeDbmsError(error);
          if (normalized.sqlState === "57014") {
            void dbms.cancel(operation.requestId).catch(() => undefined);
            void Promise.resolve(iterator.return?.()).catch(() => undefined);
          }
          throw normalized;
        }
        if (next.done) break;
        const event = next.value;
        switch (event.kind) {
          case "schema":
            set({ columns: event.columns });
            break;
          case "batch":
            set((state) => ({
              resultBuffer: appendResultRows(
                state.resultBuffer,
                event.rows,
                resultLimits,
              ),
            }));
            break;
          case "documents":
            set((state) => {
              const appended = appendStructuredValues(
                state.documentResults,
                event.documents,
                state.structuredResultBytes,
                state.droppedStructuredItems,
                state.settings,
              );
              return {
                documentResults: appended.items,
                structuredResultBytes: appended.bytes,
                droppedStructuredItems: appended.droppedItems,
              };
            });
            break;
          case "keyValues":
            set((state) => {
              const appended = appendStructuredValues(
                state.keyValueResults,
                event.entries,
                state.structuredResultBytes,
                state.droppedStructuredItems,
                state.settings,
              );
              return {
                keyValueResults: appended.items,
                structuredResultBytes: appended.bytes,
                droppedStructuredItems: appended.droppedItems,
              };
            });
            break;
          case "progress":
            set({ rowsProcessed: event.rowsProcessed });
            break;
          case "notice":
            set((state) => ({ logs: [...state.logs, event.message] }));
            break;
          case "complete":
            terminal = true;
            set((state) => ({
              queryState: "success",
              durationMs: event.durationMs,
              activeRequestId: null,
              logs: [...state.logs, event.commandTag],
              notice: `${event.commandTag} · ${resultItemCount(state)} 项`,
            }));
            break;
          case "error":
            terminal = true;
            setQueryError(set, event.error);
            break;
        }
      }
      if (!terminal) {
        setQueryError(
          set,
          localError("XX000", "查询事件流在 Complete 之前结束"),
        );
      }
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
    }
  },

  runExplain: async () => {
    const connection = get().connection;
    if (connection && !connection.capabilities.explain) {
      setQueryError(set, localError("0A000", "当前数据源不支持执行计划"));
      return;
    }
    const sql = get().sql.trim();
    if (!sql) {
      setQueryError(set, localError("42601", "SQL 不能为空"));
      return;
    }
    await get().runQuery({ sql: `EXPLAIN ${sql}`, resultTab: "plan" });
  },

  cancelQuery: async () => {
    const requestId = get().activeRequestId;
    if (!requestId) return;
    try {
      await dbms.cancel(requestId);
      set({ notice: "已发送取消请求" });
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
    }
  },

  beginTransaction: async () => {
    await runTransaction("begin", dbms, set, get);
  },
  commitTransaction: async () => {
    await runTransaction("commit", dbms, set, get);
  },
  rollbackTransaction: async () => {
    await runTransaction("rollback", dbms, set, get);
  },

  checkpoint: async () => {
    const connection = get().connection;
    if (!connection) {
      set({ dataSourceOpen: true, notice: "请先连接数据源" });
      return;
    }
    try {
      const storage = await dbms.checkpoint(connection.connectionId);
      set((state) => ({
        monitor: state.monitor
          ? {
              ...state.monitor,
              storage,
              wal: storage,
            }
          : null,
        notice: `检查点完成 · LSN ${storage.durableLsn ?? "—"}`,
      }));
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },
  }));
}

export const useWorkbenchStore = createWorkbenchStore();

type StoreSet = typeof useWorkbenchStore.setState;
type StoreGet = typeof useWorkbenchStore.getState;

function setCatalog(
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

function setQueryError(set: StoreSet, error: DbmsError) {
  set({
    queryState: "error",
    error,
    errorMessage: error.message,
    activeRequestId: null,
    activeResultTab: "logs",
    notice: `命令失败 · ${error.sqlState}`,
  });
}

async function runTransaction(
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

function localError(sqlState: string, message: string): DbmsError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: `console-${Date.now()}`,
  };
}

function withTimeout<T>(
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

function runConnectionStep<T>(
  label: string,
  operation: PromiseLike<T>,
  get: StoreGet,
) {
  const timeoutMs = get().settings.connections.timeoutMs;
  return withTimeout(operation, timeoutMs, () =>
    localError("08001", `${label}超过 ${Math.ceil(timeoutMs / 1_000)} 秒`),
  );
}

function queryTimeoutError(timeoutMs: number) {
  return localError(
    "57014",
    `命令超过 ${Math.ceil(timeoutMs / 1_000)} 秒，已请求取消`,
  );
}

function resultBufferLimits(settings: ConsoleSettingsV2): ResultBufferLimits {
  return {
    pageRows: settings.results.pageSize,
    maxRows: settings.results.residentRowLimit,
    maxBytes: settings.results.residentMemoryBytes,
  };
}

function catalogObjectIdentity(object: DbmsCatalogObject) {
  return object.id ?? `${object.kind}:${object.schema}:${object.name}`;
}

const MAX_CONNECTOR_TEXT_BYTES = 1024 * 1024;
const MAX_CONNECTOR_COMMAND_ARGUMENTS = 4096;

function buildDbmsCommand(
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

function parseRedisArguments(input: string): string[] {
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

function appendStructuredValues<T>(
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

function estimateJsonBytes(value: unknown) {
  try {
    return new TextEncoder().encode(JSON.stringify(value) ?? "null").byteLength;
  } catch {
    return MAX_CONNECTOR_TEXT_BYTES;
  }
}

function resultItemCount(state: WorkbenchState) {
  return (
    state.resultBuffer.totalRows +
    state.documentResults.length +
    state.keyValueResults.length +
    state.droppedStructuredItems
  );
}

function supportsMonitor(connection: DbmsConnectionSnapshot) {
  const capabilities = connection.capabilities;
  return (
    capabilities.sessions ||
    capabilities.locks ||
    capabilities.metrics ||
    capabilities.wal
  );
}

function loadCatalog(dbms: DbmsClient, connection: DbmsConnectionSnapshot) {
  return connection.capabilities.catalog
    ? dbms.catalog(connection.connectionId)
    : Promise.resolve({ connectionId: connection.connectionId, objects: [] });
}

function loadMonitor(dbms: DbmsClient, connection: DbmsConnectionSnapshot) {
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

function requiresDangerousWriteConfirmation(sql: string) {
  return !READ_ONLY_SQL_KEYWORDS.has(leadingSqlKeyword(sql));
}

function leadingSqlKeyword(sql: string) {
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

function replaceOperation(
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

interface SessionSaveController {
  timer?: ReturnType<typeof setTimeout>;
  autoSaveTimer?: ReturnType<typeof setTimeout>;
}

function applyConsoleSettings(settings: ConsoleSettingsV2) {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  root.style.setProperty("--font-ui", `${settings.appearance.uiFontSize}px`);
  root.style.setProperty("--font-data", `${settings.appearance.dataFontSize}px`);
  root.style.setProperty("--font-editor", `${settings.editor.fontSize}px`);
  root.style.setProperty("--ui-zoom", `${settings.appearance.zoomPercent / 100}`);
  root.style.setProperty("--editor-font-family", settings.editor.fontFamily);
  root.dataset.density = settings.appearance.density;
  root.dataset.theme = settings.appearance.theme;
  root.dataset.reduceMotion = String(settings.appearance.reduceMotion);
  root.style.colorScheme =
    settings.appearance.theme === "system"
      ? "light dark"
      : settings.appearance.theme;
}

function toOpenDocument(
  document: Omit<OpenSqlDocument, "savedContent" | "dirty" | "conflict">,
): OpenSqlDocument {
  return {
    ...document,
    savedContent: document.content,
    dirty: false,
    conflict: false,
  };
}

function prepareDocumentForSave(
  document: OpenSqlDocument,
  settings: ConsoleSettingsV2,
  dialect: SqlDialect,
  allowFormatting: boolean,
) {
  if (!settings.editor.formatOnSave || !allowFormatting) return document;
  const content = formatSqlForDialect(document.content, getSqlDialect(dialect));
  return content === document.content ? document : { ...document, content };
}

function activateSqlDocument(
  document: SqlDocument,
  set: StoreSet,
  get: StoreGet,
  status: string,
) {
  const key = documentLocatorKey(document.locator);
  const existing = get().documents.find(
    (candidate) => documentLocatorKey(candidate.locator) === key,
  );
  if (existing) {
    set({
      activeDocumentPath: existing.path,
      sql: existing.content,
      recentFiles: addRecentFile(get().recentFiles, document),
      notice: `${existing.name} · 已切换`,
    });
    return;
  }
  const open = toOpenDocument(document);
  set((state) => ({
    documents: [...state.documents, open],
    activeDocumentPath: open.path,
    sql: open.content,
    recentFiles: addRecentFile(state.recentFiles, document),
    notice: `${open.name} · ${status}`,
  }));
}

async function saveOpenDocument(
  consoleClient: ConsoleClient,
  document: OpenSqlDocument,
  force = false,
) {
  switch (document.locator.kind) {
    case "workspace":
      return consoleClient.saveDocument(
        document.locator.rootPath,
        document,
        force,
      );
    case "external":
      return consoleClient.saveExternalDocument(document, force);
    case "untitled":
      throw localError("55000", "未命名文档需要先选择保存位置");
  }
}

async function saveNamedDocument(
  path: string,
  force: boolean,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  const document = get().documents.find((candidate) => candidate.path === path);
  if (!document || document.locator.kind === "untitled") return;
  const prepared = prepareDocumentForSave(
    document,
    get().settings,
    get().dialect,
    get().connection?.connectorKind !== "document" &&
      get().connection?.connectorKind !== "keyValue",
  );
  try {
    const saved = await saveOpenDocument(consoleClient, prepared, force);
    set((state) => {
      const active = state.activeDocumentPath === path;
      return {
        documents: state.documents.map((candidate) =>
          candidate.path === path ? toOpenDocument(saved) : candidate,
        ),
        activeDocumentPath: active ? saved.path : state.activeDocumentPath,
        sql: active ? saved.content : state.sql,
        recentFiles: addRecentFile(state.recentFiles, saved),
        notice: `${saved.name} · 已保存`,
      };
    });
    await persistSession(consoleClient, get);
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    set((state) => ({
      documents: state.documents.map((candidate) =>
        candidate.path === path
          ? { ...candidate, conflict: normalized.sqlState === "40001" }
          : candidate,
      ),
      notice: normalized.message,
    }));
    throw normalized;
  }
}

function addRecentFile(
  recentFiles: RecentFileEntry[],
  document: SqlDocument,
): RecentFileEntry[] {
  const key = documentLocatorKey(document.locator);
  return [
    {
      locator: document.locator,
      name: document.name,
      openedAtMs: Date.now(),
    },
    ...recentFiles.filter(
      (entry) => documentLocatorKey(entry.locator) !== key,
    ),
  ].slice(0, 50);
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

function sameRevision(
  left: OpenSqlDocument["revision"],
  right: OpenSqlDocument["revision"],
) {
  if (!left || !right) return left === right;
  return (
    left.sizeBytes === right.sizeBytes &&
    left.modifiedAtMs === right.modifiedAtMs &&
    left.sha256 === right.sha256
  );
}

async function activateWorkspace(
  snapshot: WorkspaceSnapshot,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  set((state) => {
    const documents = state.documents.filter(
      (document) => document.locator.kind !== "workspace",
    );
    const active = documents.find(
      (document) => document.path === state.activeDocumentPath,
    );
    return {
      workspace: snapshot,
      documents,
      activeDocumentPath: active?.path ?? null,
      sql: active?.content ?? "",
      recovery: null,
      notice: `${snapshot.rootPath} · 项目已打开`,
    };
  });
  await persistSession(consoleClient, get);
}

function emptyWorkspaceSession(): WorkspaceSessionV1 {
  return {
    formatVersion: 1,
    rootPath: null,
    activePath: null,
    openDocuments: [],
  };
}

function workspaceSession(state: WorkbenchState): WorkspaceSessionV1 {
  return {
    formatVersion: 1,
    rootPath: state.workspace?.rootPath ?? null,
    activePath: state.activeDocumentPath,
    openDocuments: state.documents.map((document) => ({
      path: document.path,
      locator: document.locator,
      name: document.name,
      content: document.content,
      baseRevision: document.revision,
    })),
  };
}

function scheduleSessionSave(
  controller: SessionSaveController,
  consoleClient: ConsoleClient,
  get: StoreGet,
) {
  if (controller.timer) clearTimeout(controller.timer);
  controller.timer = setTimeout(() => {
    controller.timer = undefined;
    void persistSession(consoleClient, get);
  }, 500);
}

function clearDocumentAutoSave(controller: SessionSaveController) {
  if (!controller.autoSaveTimer) return;
  clearTimeout(controller.autoSaveTimer);
  controller.autoSaveTimer = undefined;
}

function scheduleDocumentAutoSave(
  controller: SessionSaveController,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  clearDocumentAutoSave(controller);
  const state = get();
  const document = state.documents.find(
    (candidate) => candidate.path === state.activeDocumentPath,
  );
  if (
    state.settings.files.autoSave !== "afterDelay" ||
    !document?.dirty ||
    document.conflict ||
    document.locator.kind === "untitled"
  ) {
    return;
  }
  const activePath = document.path;
  controller.autoSaveTimer = setTimeout(async () => {
    controller.autoSaveTimer = undefined;
    const current = get();
    const pending = current.documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (
      !pending?.dirty ||
      pending.conflict ||
      pending.locator.kind === "untitled"
    ) {
      return;
    }
    await saveNamedDocument(
      activePath,
      false,
      consoleClient,
      set,
      get,
    ).catch(() => undefined);
  }, state.settings.files.autoSaveDelayMs);
}

async function persistSession(consoleClient: ConsoleClient, get: StoreGet) {
  try {
    await consoleClient.saveSession(workspaceSession(get()));
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    get().setNotice(`草稿恢复状态保存失败 · ${normalized.message}`);
  }
}

function nextUntitledName(documents: OpenSqlDocument[]) {
  const names = new Set(documents.map((document) => document.name.toLowerCase()));
  for (let sequence = 1; sequence <= 9_999; sequence += 1) {
    const name = `未命名-${sequence}.sql`;
    if (!names.has(name.toLowerCase())) return name;
  }
  return `未命名-${Date.now()}.sql`;
}

function nextUntitledId(documents: OpenSqlDocument[]) {
  const ids = new Set(
    documents
      .filter(
        (
          document,
        ): document is OpenSqlDocument & {
          locator: Extract<DocumentLocator, { kind: "untitled" }>;
        } => document.locator.kind === "untitled",
      )
      .map((document) => document.locator.id),
  );
  for (let sequence = 1; sequence <= 9_999; sequence += 1) {
    const id = `untitled-${sequence}`;
    if (!ids.has(id)) return id;
  }
  return `untitled-${Date.now()}`;
}

function renamedPath(path: string, newName: string) {
  const separator = path.lastIndexOf("/");
  return separator < 0 ? newName : `${path.slice(0, separator + 1)}${newName}`;
}

function replacePathPrefix(path: string, before: string, after: string) {
  if (path === before) return after;
  return path.startsWith(`${before}/`)
    ? `${after}${path.slice(before.length)}`
    : path;
}

async function connectProfile(
  profile: ConnectionProfileV3,
  dbms: DbmsClient,
  _consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  set({
    connectionState: "connecting",
    connectionError: null,
    activeCredentialId: profile.credentialId,
    notice: `正在重连 ${profile.label}`,
  });
  const request = toConnectionRequest(profile);
  try {
    const probe = await runConnectionStep(
      "自动重连诊断",
      dbms.probe(request),
      get,
    );
    set({ connectionProbe: probe });
    if (!probe.ready) {
      throw (
        probe.stages.find(
          (stage) => stage.status === "failed" && stage.stage !== "service",
        )?.error ?? localError("08001", "本地数据库自动重连诊断未通过")
      );
    }
    const connection = await runConnectionStep(
      "自动重连",
      dbms.connect(request),
      get,
    );
    let catalog: Awaited<ReturnType<DbmsClient["catalog"]>>;
    let monitor: Awaited<ReturnType<DbmsClient["monitor"]>> | null;
    try {
      [catalog, monitor] = await runConnectionStep(
        "自动重连元数据加载",
        Promise.all([
          loadCatalog(dbms, connection),
          loadMonitor(dbms, connection),
        ]),
        get,
      );
    } catch (error) {
      void dbms.disconnect(connection.connectionId).catch(() => undefined);
      throw error;
    }
    set({
      connection,
      monitor,
      dialect: connection.dialect ?? get().dialect,
      connectionState: "connected",
      connectionError: null,
      notice: `${connection.database} · 已自动重连`,
    });
    setCatalog(set, get, catalog.objects);
    await get().refreshAdministration();
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    set({
      connectionState: "error",
      connectionError: normalized,
      notice: `自动重连失败 · ${normalized.sqlState}`,
    });
  }
}

function toConnectionRequest(
  values:
    | DataSourceValues
    | Pick<
        ConnectionProfileV3,
        | "connectorId"
        | "connectorKind"
        | "commandLanguage"
        | "dialect"
        | "endpoint"
        | "adminEndpoint"
        | "database"
        | "tlsMode"
        | "credentialId"
      >,
): DbmsConnectionRequest {
  return {
    connectorId: values.connectorId,
    connectorKind: values.connectorKind,
    commandLanguage: values.commandLanguage,
    dialect: values.dialect,
    endpoint: values.endpoint,
    adminEndpoint: values.adminEndpoint,
    database: values.database,
    tlsMode: values.tlsMode,
    credentialId: values.credentialId,
  };
}

function failedProbeError(probe: ConnectionProbe) {
  return probe.stages.find((stage) => stage.status === "failed")?.error ?? null;
}

function requireNativePreflight(probe: ConnectionProbe) {
  const blocking = probe.stages.find(
    (stage) =>
      stage.status === "failed" &&
      ["service", "pgPort", "adminApi", "initialization"].includes(stage.stage),
  );
  if (blocking) {
    throw (
      blocking.error ?? localError("08001", "本地 OrdaDB 连接前置诊断未通过")
    );
  }
}

function requireReadyProbe(probe: ConnectionProbe, fallbackMessage: string) {
  if (!probe.ready) {
    throw failedProbeError(probe) ?? localError("08001", fallbackMessage);
  }
}

async function establishDataSourceConnection(
  values: DataSourceValues,
  request: DbmsConnectionRequest,
  dbms: DbmsClient,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  const descriptor = get().connectorDescriptors.find(
    (candidate) => candidate.connectorId === values.connectorId,
  );
  if (!descriptor) {
    throw localError("22023", "未知的数据源类型");
  }
  const connection = await runConnectionStep(
    "建立数据库连接",
    dbms.connect(request),
    get,
  );
  let catalog: Awaited<ReturnType<DbmsClient["catalog"]>>;
  let monitor: Awaited<ReturnType<DbmsClient["monitor"]>> | null;
  try {
    [catalog, monitor] = await runConnectionStep(
      "加载数据库元数据",
      Promise.all([
        loadCatalog(dbms, connection),
        loadMonitor(dbms, connection),
      ]),
      get,
    );
  } catch (error) {
    void dbms.disconnect(connection.connectionId).catch(() => undefined);
    throw error;
  }
  const profiles = await consoleClient.saveConnectionProfile({
    formatVersion: 3,
    profileId: values.credentialId,
    label: values.database || values.endpoint,
    dataSourceKind: descriptor.dataSourceKind,
    connectorId: values.connectorId,
    connectorKind: descriptor.connectorKind,
    commandLanguage: descriptor.commandLanguage,
    dialect: values.dialect,
    endpoint: values.endpoint,
    adminEndpoint: values.adminEndpoint,
    database: values.database,
    tlsMode: values.tlsMode,
    credentialId: values.credentialId,
    autoReconnect:
      values.connectorId === "ordadb-native" &&
      get().settings.connections.autoReconnectLocal,
  });
  set({
    connection,
    monitor,
    operations: [],
    serviceStatus: null,
    dialect: connection.dialect ?? get().dialect,
    dataSourceOpen: false,
    connectionState: "connected",
    connectionError: null,
    transactionActive: false,
    connectionProfiles: profiles,
    notice: `${connection.database} · 已连接`,
  });
  setCatalog(set, get, catalog.objects);
  await get().refreshAdministration();
}

function operationLabel(kind: DbmsOperationRecord["kind"]) {
  switch (kind) {
    case "backup":
      return "备份";
    case "restore":
      return "恢复";
    case "import":
      return "导入";
    case "export":
      return "导出";
  }
}
