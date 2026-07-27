import { create } from "zustand";
import { getSqlDialect } from "../data/dialects";
import { initialSql } from "../data/preview";
import {
  getDbmsClient,
  normalizeDbmsError,
  previewConnection,
  type DbmsCatalogObject,
  type DbmsClient,
  type DbmsConnectionRequest,
  type DbmsConnectionSnapshot,
  type DbmsError,
  type DbmsMonitorSnapshot,
  type DbmsQueryColumn,
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
  connectionState: "idle" | "connecting" | "connected" | "error";
  connectionError: DbmsError | null;
  transactionActive: boolean;
  initialize: () => Promise<void>;
  setSql: (sql: string) => void;
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
  connectDataSource: (values: DataSourceValues) => Promise<void>;
  disconnectDataSource: () => Promise<void>;
  deleteStoredCredential: () => Promise<void>;
  refreshCatalog: () => Promise<void>;
  refreshMonitor: () => Promise<void>;
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
) {
  return create<WorkbenchState>((set, get) => ({
  sql: initialSql,
  dialect: "postgresql",
  schemaVisible: true,
  inspectorVisible: true,
  activeResultTab: "data",
  activeInspectorTab: "properties",
  selectedObject: "documents",
  selectedCatalogObject: null,
  commandPaletteOpen: false,
  pluginManagerOpen: false,
  dataSourceOpen: false,
  operationsOpen: false,
  operationView: "sessions",
  notice:
    dbms.mode === "preview"
      ? "Preview fixture · 不连接真实数据库"
      : "请选择数据源",
  queryState: "idle",
  columns: [],
  rows: [],
  logs: [],
  error: null,
  errorMessage: null,
  durationMs: null,
  rowsProcessed: 0,
  activeRequestId: null,
  connection: dbms.mode === "preview" ? previewConnection : null,
  activeCredentialId: null,
  catalog: [],
  monitor: null,
  connectionState: dbms.mode === "preview" ? "connected" : "idle",
  connectionError: null,
  transactionActive: false,

  initialize: async () => {
    const connection = get().connection;
    if (!connection || get().catalog.length > 0) return;
    try {
      const [catalog, monitor] = await Promise.all([
        dbms.catalog(connection.connectionId),
        dbms.monitor(connection.connectionId),
      ]);
      setCatalog(set, get, catalog.objects);
      set({ monitor });
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        connectionState: "error",
        connectionError: normalized,
        notice: normalized.message,
      });
    }
  },

  setSql: (sql) => set({ sql }),
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

  openOperations: async (operationView) => {
    set({ operationView, operationsOpen: true });
    await get().refreshMonitor();
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
      const connection = await dbms.connect({
        connectorId: values.connectorId,
        dialect: values.dialect,
        endpoint: values.endpoint,
        adminEndpoint: values.adminEndpoint,
        database: values.database,
        credentialId: values.credentialId,
      });
      const [catalog, monitor] = await Promise.all([
        dbms.catalog(connection.connectionId),
        dbms.monitor(connection.connectionId),
      ]);
      set({
        connection,
        monitor,
        dialect: connection.dialect,
        dataSourceOpen: false,
        connectionState: "connected",
        connectionError: null,
        transactionActive: false,
        notice: `${connection.database} · 已连接`,
      });
      setCatalog(set, get, catalog.objects);
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
      set({
        activeCredentialId: null,
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
