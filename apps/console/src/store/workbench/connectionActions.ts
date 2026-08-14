import type {
  ConnectionProfileV3,
  ConsoleClient,
} from "../../lib/consoleClient";
import {
  normalizeDbmsError,
  type ConnectionProbe,
  type DbmsClient,
  type DbmsConnectionRequest,
  type DbmsOperationRecord,
} from "../../lib/dbmsClient";
import type { WorkbenchActionContext, StoreGet, StoreSet } from "./context";
import {
  loadCatalog,
  loadMonitor,
  localError,
  replaceOperation,
  runConnectionStep,
  setCatalog,
  supportsMonitor,
} from "./databaseSupport";
import type { DataSourceValues, WorkbenchState } from "./types";

export function createConnectionActions({
  consoleClient,
  dbms,
  get,
  set,
}: WorkbenchActionContext) {
  return {  bootstrapAdministrator: async (values) => {
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

  } satisfies Partial<WorkbenchState>;
}
export async function connectProfile(
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
        | "credentialAccess"
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
    credentialAccess: values.credentialAccess ?? "unspecified",
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
    credentialAccess: values.credentialAccess ?? "unspecified",
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
