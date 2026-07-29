import { create } from "zustand";
import { getSqlDialect } from "../data/dialects";
import {
  defaultConsoleSettings,
  getConsoleClient,
  type ConnectionProfileV1,
  type ConsoleClient,
  type ConsoleSettingsV1,
  type OpenSqlDocument,
  type WorkspaceSessionV1,
  type WorkspaceSnapshot,
} from "../lib/consoleClient";
import {
  getDbmsClient,
  normalizeDbmsError,
  type BootstrapAdminRequest,
  type ConnectionProbe,
  type DbmsCatalogObject,
  type DbmsClient,
  type DbmsConnectionRequest,
  type DbmsConnectionSnapshot,
  type DbmsError,
  type DbmsMonitorSnapshot,
  type DbmsOperationRecord,
  type DbmsQueryColumn,
  type DbmsServiceStatus,
  type StartDbmsOperationRequest,
} from "../lib/dbmsClient";
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

export interface DataSourceValues extends DbmsConnectionRequest {
  username: string;
  password: string;
}

interface RunQueryOptions {
  sql?: string;
  resultTab?: ResultTab;
}

export interface WorkbenchState {
  sql: string;
  settings: ConsoleSettingsV1;
  settingsOpen: boolean;
  workspace: WorkspaceSnapshot | null;
  documents: OpenSqlDocument[];
  activeDocumentPath: string | null;
  recovery: WorkspaceSessionV1 | null;
  connectionProfiles: ConnectionProfileV1[];
  connectionProbe: ConnectionProbe | null;
  dialect: SqlDialect;
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
  rows: Array<Array<string | null>>;
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
  saveSettings: (settings: ConsoleSettingsV1) => Promise<void>;
  openWorkspace: () => Promise<void>;
  openWorkspacePath: (rootPath: string) => Promise<void>;
  restoreRecovery: () => Promise<void>;
  discardRecovery: () => Promise<void>;
  openDocument: (path: string) => Promise<void>;
  createDocument: (parentPath?: string) => Promise<void>;
  activateDocument: (path: string) => void;
  closeDocument: (path: string) => Promise<void>;
  reloadActiveDocument: () => Promise<void>;
  saveActiveDocument: (force?: boolean) => Promise<void>;
  saveAllDocuments: () => Promise<void>;
  renameWorkspaceEntry: (path: string, newName: string) => Promise<void>;
  trashWorkspaceEntry: (path: string) => Promise<void>;
  setDialect: (dialect: SqlDialect) => void;
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
  bootstrapAdministrator: (request: BootstrapAdminRequest) => Promise<void>;
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
  sql: "",
  settings: { ...defaultConsoleSettings },
  settingsOpen: false,
  workspace: null,
  documents: [],
  activeDocumentPath: null,
  recovery: null,
  connectionProfiles: [],
  connectionProbe: null,
  dialect: "postgresql",
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
  rows: [],
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
        connectionProfiles: bootstrap.connectionProfiles,
      });
      applyConsoleSettings(bootstrap.settings);
      if (bootstrap.settings.reopenLastProject && bootstrap.recovery) {
        await get().restoreRecovery();
      }
      const reconnect = bootstrap.connectionProfiles.find(
        (profile) => profile.autoReconnect,
      );
      if (reconnect && dbms.mode === "desktop") {
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
              dirty: sql !== document.savedContent,
            }
          : document,
      ),
    }));
    scheduleSessionSave(sessionSaveController, consoleClient, get);
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
  restoreRecovery: async () => {
    const recovery = get().recovery;
    if (!recovery?.rootPath) return;
    try {
      const snapshot = await consoleClient.openWorkspace(recovery.rootPath);
      const documents: OpenSqlDocument[] = [];
      for (const draft of recovery.openDocuments) {
        const current = await consoleClient.openDocument(
          snapshot.rootPath,
          draft.path,
        );
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
      const openDocument = toOpenDocument(document);
      set((state) => ({
        documents: [...state.documents, openDocument],
        activeDocumentPath: openDocument.path,
        sql: openDocument.content,
        notice: `${openDocument.name} · 已打开`,
      }));
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  createDocument: async (parentPath = "") => {
    let workspace = get().workspace;
    if (!workspace) {
      await get().openWorkspace();
      workspace = get().workspace;
    }
    if (!workspace) return;
    const fileName = nextQueryFileName(workspace);
    try {
      const document = await consoleClient.newDocument(
        workspace.rootPath,
        parentPath,
        fileName,
      );
      const snapshot = await consoleClient.openWorkspace(workspace.rootPath);
      const openDocument = toOpenDocument(document);
      set((state) => ({
        workspace: snapshot,
        documents: [...state.documents, openDocument],
        activeDocumentPath: openDocument.path,
        sql: "",
        notice: `${openDocument.name} · 已创建`,
      }));
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
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
    const workspace = get().workspace;
    const activePath = get().activeDocumentPath;
    if (!workspace || !activePath) return;
    try {
      const document = await consoleClient.openDocument(
        workspace.rootPath,
        activePath,
      );
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
    const workspace = get().workspace;
    const activePath = get().activeDocumentPath;
    const document = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!workspace || !document) return;
    try {
      const saved = await consoleClient.saveDocument(
        workspace.rootPath,
        document,
        force,
      );
      set((state) => ({
        documents: state.documents.map((candidate) =>
          candidate.path === saved.path ? toOpenDocument(saved) : candidate,
        ),
        sql: saved.content,
        notice: `${saved.name} · 已保存`,
      }));
      await persistSession(consoleClient, get);
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
      throw normalized;
    }
  },
  saveAllDocuments: async () => {
    const workspace = get().workspace;
    if (!workspace) return;
    for (const document of get().documents.filter((candidate) => candidate.dirty)) {
      try {
        const saved = await consoleClient.saveDocument(
          workspace.rootPath,
          document,
        );
        set((state) => ({
          documents: state.documents.map((candidate) =>
            candidate.path === saved.path ? toOpenDocument(saved) : candidate,
          ),
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
        const pathAfterRename = replacePathPrefix(document.path, path, nextPath);
        return {
          ...document,
          path: pathAfterRename,
          name: pathAfterRename.split("/").at(-1) ?? document.name,
        };
      });
      const activeDocumentPath = get().activeDocumentPath
        ? replacePathPrefix(get().activeDocumentPath!, path, nextPath)
        : null;
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
          document.path !== path && !document.path.startsWith(`${path}/`),
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
  toggleSchema: () => set((state) => ({ schemaVisible: !state.schemaVisible })),
  toggleInspector: () =>
    set((state) => ({ inspectorVisible: !state.inspectorVisible })),
  setActiveResultTab: (activeResultTab) => set({ activeResultTab }),
  setActiveInspectorTab: (activeInspectorTab) => set({ activeInspectorTab }),
  setSelectedObject: (selectedObject) =>
    set({
      selectedObject,
      selectedCatalogObject:
        get().catalog.find((object) => object.name === selectedObject) ?? null,
    }),
  setCommandPaletteOpen: (commandPaletteOpen) => set({ commandPaletteOpen }),
  setPluginManagerOpen: (pluginManagerOpen) => set({ pluginManagerOpen }),
  setDataSourceOpen: (dataSourceOpen) => set({ dataSourceOpen }),
  setOperationsOpen: (operationsOpen) => set({ operationsOpen }),
  setNotice: (notice) => set({ notice }),

  bootstrapAdministrator: async (request) => {
    try {
      const result = await dbms.bootstrapAdmin(request);
      if (!result.success || result.error) {
        throw result.error ?? localError("55000", "管理员初始化未完成");
      }
      set({
        notice: `${result.user ?? request.username} · 管理员初始化完成`,
        connectionProbe: null,
      });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
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
      await dbms.saveCredential({
        credentialId: values.credentialId,
        username: values.username,
        password: values.password,
      });
      set({ activeCredentialId: values.credentialId });
      const request = {
        connectorId: values.connectorId,
        dialect: values.dialect,
        endpoint: values.endpoint,
        adminEndpoint: values.adminEndpoint,
        database: values.database,
        credentialId: values.credentialId,
      };
      const probe = await dbms.probe(request);
      set({ connectionProbe: probe });
      const failed = probe.stages.find(
        (stage) => stage.status === "failed" && stage.stage !== "service",
      );
      if (!probe.ready) {
        throw (
          failed?.error ??
          localError("08001", "本地数据库连接诊断未通过")
        );
      }
      const connection = await dbms.connect(request);
      const [catalog, monitor] = await Promise.all([
        dbms.catalog(connection.connectionId),
        dbms.monitor(connection.connectionId),
      ]);
      set({
        connection,
        monitor,
        operations: [],
        serviceStatus: null,
        dialect: connection.dialect,
        dataSourceOpen: false,
        connectionState: "connected",
        connectionError: null,
        transactionActive: false,
        notice: `${connection.database} · 已连接`,
      });
      setCatalog(set, get, catalog.objects);
      const profiles = await consoleClient.saveConnectionProfile({
        formatVersion: 1,
        profileId: values.credentialId,
        label: values.database || values.endpoint,
        connectorId: values.connectorId,
        dialect: values.dialect,
        endpoint: values.endpoint,
        adminEndpoint: values.adminEndpoint,
        database: values.database,
        credentialId: values.credentialId,
        autoReconnect: values.connectorId === "ordadb-native",
      });
      set({ connectionProfiles: profiles });
      await get().refreshAdministration();
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
    const sql = (options.sql ?? get().sql).trim();
    const connection = get().connection;
    if (!sql) {
      const error = localError("42601", "SQL 不能为空");
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
    set({
      queryState: "running",
      columns: [],
      rows: [],
      logs: [],
      error: null,
      errorMessage: null,
      durationMs: null,
      rowsProcessed: 0,
      activeRequestId: null,
      activeResultTab: options.resultTab ?? "data",
      notice:
        connection.mode === "preview" ? "正在运行 Preview 查询" : "正在运行查询",
    });
    try {
      const operation = await dbms.execute(connection.connectionId, sql);
      set({ activeRequestId: operation.requestId });
      let terminal = false;
      for await (const event of operation.events) {
        switch (event.kind) {
          case "schema":
            set({ columns: event.columns });
            break;
          case "batch":
            set((state) => ({ rows: [...state.rows, ...event.rows] }));
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
              notice: `${event.commandTag} · ${state.rows.length} 行`,
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
    catalog.find((object) => object.name === current) ??
    catalog.find((object) => object.kind === "table") ??
    catalog[0] ??
    null;
  set({
    catalog,
    selectedObject: selected?.name ?? "",
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
    notice: `查询失败 · ${error.sqlState}`,
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
}

function applyConsoleSettings(settings: ConsoleSettingsV1) {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  root.style.setProperty("--font-ui", `${settings.uiFontSize}px`);
  root.style.setProperty("--font-data", `${settings.dataFontSize}px`);
  root.style.setProperty("--font-editor", `${settings.editorFontSize}px`);
  root.dataset.density = settings.density;
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

function sameRevision(
  left: OpenSqlDocument["revision"],
  right: OpenSqlDocument["revision"],
) {
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
  set({
    workspace: snapshot,
    documents: [],
    activeDocumentPath: null,
    sql: "",
    recovery: null,
    notice: `${snapshot.rootPath} · 项目已打开`,
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
  if (!state.workspace) return emptyWorkspaceSession();
  return {
    formatVersion: 1,
    rootPath: state.workspace.rootPath,
    activePath: state.activeDocumentPath,
    openDocuments: state.documents.map((document) => ({
      path: document.path,
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

async function persistSession(consoleClient: ConsoleClient, get: StoreGet) {
  try {
    await consoleClient.saveSession(workspaceSession(get()));
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    get().setNotice(`草稿恢复状态保存失败 · ${normalized.message}`);
  }
}

function nextQueryFileName(workspace: WorkspaceSnapshot) {
  const names = new Set(
    workspace.entries
      .filter((entry) => entry.kind === "sqlFile")
      .map((entry) => entry.name.toLowerCase()),
  );
  if (!names.has("query.sql")) return "query.sql";
  for (let index = 2; index <= 9_999; index += 1) {
    const name = `query_${String(index).padStart(2, "0")}.sql`;
    if (!names.has(name.toLowerCase())) return name;
  }
  return `query_${Date.now()}.sql`;
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
  profile: ConnectionProfileV1,
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
  const request = {
    connectorId: profile.connectorId,
    dialect: profile.dialect,
    endpoint: profile.endpoint,
    adminEndpoint: profile.adminEndpoint,
    database: profile.database,
    credentialId: profile.credentialId,
  };
  try {
    const probe = await dbms.probe(request);
    set({ connectionProbe: probe });
    if (!probe.ready) {
      throw (
        probe.stages.find(
          (stage) => stage.status === "failed" && stage.stage !== "service",
        )?.error ?? localError("08001", "本地数据库自动重连诊断未通过")
      );
    }
    const connection = await dbms.connect(request);
    const [catalog, monitor] = await Promise.all([
      dbms.catalog(connection.connectionId),
      dbms.monitor(connection.connectionId),
    ]);
    set({
      connection,
      monitor,
      dialect: connection.dialect,
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
