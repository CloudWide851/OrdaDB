import { create } from "zustand";
import {
  cloneConsoleSettings,
  defaultConnectorDescriptors,
  defaultConsoleSettings,
  getConsoleClient,
  type ConsoleClient,
} from "../lib/consoleClient";
import { getDbmsClient, type DbmsClient } from "../lib/dbmsClient";
import { emptyResultBuffer } from "../lib/resultBuffer";
import { getAiClient, type AiClient } from "../lib/aiClient";
import { createAiActions } from "./workbench/aiActions";
import { createConnectionActions } from "./workbench/connectionActions";
import type { WorkbenchActionContext } from "./workbench/context";
import { createCoreActions } from "./workbench/coreActions";
import { createDocumentActions } from "./workbench/documentActions";
import type { SessionSaveController } from "./workbench/documentSupport";
import { createQueryActions } from "./workbench/queryActions";
import type { WorkbenchState } from "./workbench/types";
import { createUiActions } from "./workbench/uiActions";

export type {
  AiRunStatus,
  AiToolActivity,
  AiVisibleMessage,
  DataSourceValues,
  InspectorMode,
  OperationView,
  QuickOpenMode,
  RunQueryOptions,
  SidebarView,
  WorkbenchState,
} from "./workbench/types";

export function createWorkbenchStore(
  dbms: DbmsClient = getDbmsClient(),
  consoleClient: ConsoleClient = getConsoleClient(),
  ai: AiClient = getAiClient(),
) {
  const sessionSaveController: SessionSaveController = {};

  return create<WorkbenchState>((set, get) => {
    const context: WorkbenchActionContext = {
      ai,
      consoleClient,
      dbms,
      get,
      sessionSaveController,
      set,
    };

    return {  runtimeMode: dbms.mode,
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
  inspectorMode: "object",
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
  aiRuntimeMode: ai.mode,
  aiMessages: [],
  aiPersistedHistory: [],
  aiAudit: [],
  aiDisclosures: [],
  aiTools: [],
  aiApproval: null,
  aiUsage: null,
  aiRunId: null,
  aiRunStatus: "idle",
  aiLastSequence: 0,
  aiError: null,
  aiCredentialStatus: null,
  aiCredentialBusy: false,
  aiCredentialError: null,
      ...createCoreActions(context),
      ...createDocumentActions(context),
      ...createUiActions(context),
      ...createAiActions(context),
      ...createConnectionActions(context),
      ...createQueryActions(context),
    };
  });
}

export const useWorkbenchStore = createWorkbenchStore();
