import ordadbLogoUrl from "../../../../logo.svg?url";
import mysqlLogoUrl from "../assets/connectors/mysql.svg?url";
import postgresqlLogoUrl from "../assets/connectors/postgresql.svg?url";
import sqliteLogoUrl from "../assets/connectors/sqlite.svg?url";
import sqlServerLogoUrl from "../assets/connectors/sql-server.svg?url";
import type {
  ConnectorDescriptor,
  DataSourceKind,
} from "../lib/consoleClient";
import type { SqlDialect } from "../types";

export type ConnectorManifestDialect =
  | "postgreSql"
  | "mySql"
  | "sqlite"
  | "sqlServer";
export type ConnectorPermission = "network" | "localDatabaseFile";
export type RegistryAvailability = "configured" | "notConfigured";
export type PluginLifecycle =
  | "available"
  | "downloading"
  | "verifying"
  | "installing"
  | "installed"
  | "updateAvailable"
  | "failed";
export type ConnectorViewLifecycle = PluginLifecycle | "unavailable";
export type PluginProgressPhase =
  | "resolving"
  | "downloading"
  | "verifying"
  | "installing"
  | "complete"
  | "cancelled"
  | "failed";
export type PluginOperationKind = "install" | "retry" | "update";

export interface PluginError {
  sqlState: string;
  message: string;
  detail: string | null;
  hint: string | null;
  position: number | null;
  queryId: string;
}

export interface RegistryStatus {
  availability: RegistryAvailability;
  apiVersion: number;
  message: string;
}

export interface PluginCatalogItem {
  id: string;
  displayName: string;
  version: string;
  dialect: ConnectorManifestDialect;
  publisher: string;
  permissions: ConnectorPermission[];
  size: number;
  lifecycle: PluginLifecycle;
  installedVersion: string | null;
  previousVersion: string | null;
  operationId: string | null;
  downloadedBytes: number;
  error: PluginError | null;
}

export interface PluginCatalogSnapshot {
  registry: RegistryStatus;
  plugins: PluginCatalogItem[];
}

export interface PluginProgress {
  operationId: string;
  pluginId: string;
  kind: PluginOperationKind;
  phase: PluginProgressPhase;
  downloadedBytes: number;
  totalBytes: number | null;
  error: PluginError | null;
}

export interface PluginOperationStarted {
  operationId: string;
  pluginId: string;
  kind: PluginOperationKind;
}

export interface ConnectorDefinition {
  id: string;
  dataSourceKind: DataSourceKind;
  displayName: string;
  shortName: string;
  logoUrl: string;
  defaultEndpoint: string;
  defaultAdminEndpoint?: string;
  defaultDatabase?: string;
  defaultTlsMode: ConnectorDescriptor["defaultTlsMode"];
  dialect: ConnectorManifestDialect;
  sqlDialect: SqlDialect;
  publisher: string;
  permissions: ConnectorPermission[];
  size: number;
}

export interface ConnectorViewModel
  extends Omit<PluginCatalogItem, "lifecycle"> {
  shortName: string;
  sqlDialect: SqlDialect;
  logoUrl: string;
  lifecycle: ConnectorViewLifecycle;
}

export const connectorDefinitions: ConnectorDefinition[] = [
  {
    id: "ordadb-native",
    dataSourceKind: "ordadbNative",
    displayName: "OrdaDB",
    shortName: "OrdaDB",
    logoUrl: ordadbLogoUrl,
    defaultEndpoint: "127.0.0.1:54329",
    defaultAdminEndpoint: "http://127.0.0.1:9080",
    defaultDatabase: "ordadb",
    defaultTlsMode: "disable",
    dialect: "postgreSql",
    sqlDialect: "postgresql",
    publisher: "OrdaDB",
    permissions: ["network"],
    size: 0,
  },
  {
    id: "postgresql",
    dataSourceKind: "postgresql",
    displayName: "PostgreSQL",
    shortName: "PostgreSQL",
    logoUrl: postgresqlLogoUrl,
    defaultEndpoint: "127.0.0.1:5432",
    defaultDatabase: "postgres",
    defaultTlsMode: "prefer",
    dialect: "postgreSql",
    sqlDialect: "postgresql",
    publisher: "OrdaDB",
    permissions: ["network"],
    size: 8_388_608,
  },
  {
    id: "mysql",
    dataSourceKind: "mysql",
    displayName: "MySQL",
    shortName: "MySQL",
    logoUrl: mysqlLogoUrl,
    defaultEndpoint: "127.0.0.1:3306",
    defaultTlsMode: "prefer",
    dialect: "mySql",
    sqlDialect: "mysql",
    publisher: "OrdaDB",
    permissions: ["network"],
    size: 7_340_032,
  },
  {
    id: "sqlite",
    dataSourceKind: "sqlite",
    displayName: "SQLite",
    shortName: "SQLite",
    logoUrl: sqliteLogoUrl,
    defaultEndpoint: "",
    defaultTlsMode: "disable",
    dialect: "sqlite",
    sqlDialect: "sqlite",
    publisher: "OrdaDB",
    permissions: ["localDatabaseFile"],
    size: 4_194_304,
  },
  {
    id: "sql-server",
    dataSourceKind: "sqlServer",
    displayName: "SQL Server",
    shortName: "SQL Server",
    logoUrl: sqlServerLogoUrl,
    defaultEndpoint: "127.0.0.1:1433",
    defaultTlsMode: "require",
    dialect: "sqlServer",
    sqlDialect: "sqlServer",
    publisher: "OrdaDB",
    permissions: ["network"],
    size: 9_437_184,
  },
];

export const pluginConnectorDefinitions = connectorDefinitions.filter(
  (definition) => definition.id !== "ordadb-native",
);

const definitionById = new Map(
  connectorDefinitions.map((definition) => [definition.id, definition]),
);

export function projectConnectorCatalog(
  snapshot: PluginCatalogSnapshot,
): ConnectorViewModel[] {
  const pluginById = new Map(
    snapshot.plugins.map((plugin) => [plugin.id, plugin]),
  );
  return pluginConnectorDefinitions.map((definition) => {
    const plugin = pluginById.get(definition.id);
    if (plugin) {
      return {
        ...plugin,
        shortName: definition.shortName,
        sqlDialect: definition.sqlDialect,
        logoUrl: definition.logoUrl,
      };
    }
    return {
      ...definition,
      version: "—",
      lifecycle: "unavailable",
      installedVersion: null,
      previousVersion: null,
      operationId: null,
      downloadedBytes: 0,
      error: null,
    };
  });
}

export function getConnectorDefinition(id: string): ConnectorDefinition {
  const definition = definitionById.get(id);
  if (!definition) {
    throw new Error(`Unknown connector definition: ${id}`);
  }
  return definition;
}

export function formatConnectorBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  return `${(bytes / (1024 * 1024)).toFixed(bytes < 10 * 1024 * 1024 ? 1 : 0)} MB`;
}

export function compareSemanticVersions(left: string, right: string): number {
  const [leftCore, leftPrerelease] = left.split("-", 2);
  const [rightCore, rightPrerelease] = right.split("-", 2);
  const leftParts = leftCore.split(".").map(Number);
  const rightParts = rightCore.split(".").map(Number);
  const width = Math.max(leftParts.length, rightParts.length);

  for (let index = 0; index < width; index += 1) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return Math.sign(difference);
  }

  if (leftPrerelease === rightPrerelease) return 0;
  if (!leftPrerelease) return 1;
  if (!rightPrerelease) return -1;
  return leftPrerelease.localeCompare(rightPrerelease, undefined, {
    numeric: true,
  });
}

export const connectorPermissionLabels: Record<ConnectorPermission, string> = {
  network: "网络访问",
  localDatabaseFile: "本地数据库文件",
};
