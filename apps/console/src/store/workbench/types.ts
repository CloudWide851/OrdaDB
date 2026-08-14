import type {
  ConnectionProfileV3,
  ConnectorDescriptor,
  ConsoleSettingsV2,
  OpenSqlDocument,
  RecentFileEntry,
  WorkspaceSessionV1,
  WorkspaceSnapshot,
} from "../../lib/consoleClient";
import type {
  ConnectionProbe,
  DbmsCatalogObject,
  DbmsClient,
  DbmsConnectionRequest,
  DbmsConnectionSnapshot,
  DbmsError,
  DbmsKeyValue,
  DbmsMonitorSnapshot,
  DbmsOperationRecord,
  DbmsQueryColumn,
  DbmsServiceStatus,
  StartDbmsOperationRequest,
} from "../../lib/dbmsClient";
import type { ResultBuffer } from "../../lib/resultBuffer";
import type {
  AiApprovalRequest,
  AiAuditEntry,
  AiClient,
  AiContextDisclosure,
  AiCredentialStatus,
  AiError,
  AiHistoryEntry,
  AiUsage,
} from "../../lib/aiClient";
import type {
  InspectorTab,
  QueryState,
  ResultTab,
  SqlDialect,
} from "../../types";
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
export type InspectorMode = "object" | "ai";
export type AiRunStatus =
  | "idle"
  | "running"
  | "waitingApproval"
  | "completed"
  | "cancelled"
  | "error";

export interface AiVisibleMessage {
  id: string;
  role: "user" | "assistant";
  text: string;
  createdAtMs: number;
}
export interface AiToolActivity {
  callId: string;
  toolName: string;
  status: "proposed" | "waitingApproval" | "running" | "completed";
  summary: string | null;
  truncated: boolean;
}

export interface DataSourceValues extends DbmsConnectionRequest {
  username: string;
}

export interface RunQueryOptions {
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
  inspectorMode: InspectorMode;
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
  aiRuntimeMode: AiClient["mode"];
  aiMessages: AiVisibleMessage[];
  aiPersistedHistory: AiHistoryEntry[];
  aiAudit: AiAuditEntry[];
  aiDisclosures: AiContextDisclosure[];
  aiTools: AiToolActivity[];
  aiApproval: AiApprovalRequest | null;
  aiUsage: AiUsage | null;
  aiRunId: string | null;
  aiRunStatus: AiRunStatus;
  aiLastSequence: number;
  aiError: AiError | null;
  aiCredentialStatus: AiCredentialStatus | null;
  aiCredentialBusy: boolean;
  aiCredentialError: AiError | null;
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
  setInspectorMode: (mode: InspectorMode) => void;
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
  startAiRun: (userText: string, includeSampleValues?: boolean) => Promise<void>;
  cancelAiRun: () => Promise<void>;
  decideAiApproval: (approve: boolean) => Promise<void>;
  refreshAiCredentialStatus: (credentialId?: string) => Promise<void>;
  promptAiCredential: (
    credentialId: string,
    providerLabel: string,
  ) => Promise<AiCredentialStatus | null>;
  deleteAiCredential: (credentialId: string) => Promise<void>;
}
