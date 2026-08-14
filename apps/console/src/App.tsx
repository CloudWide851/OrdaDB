import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect } from "react";
import { Route, Routes } from "react-router-dom";
import { CommandPalette } from "./components/CommandPalette";
import { AiWorkbench } from "./components/AiWorkbench";
import { ConnectorManager } from "./components/ConnectorManager";
import { DataSourceDialog } from "./components/DataSourceDialog";
import { EditorPane } from "./components/EditorPane";
import { ObjectInspector } from "./components/ObjectInspector";
import { OperationsPanel } from "./components/OperationsPanel";
import { NavigationBar } from "./components/NavigationBar";
import { QuickOpen } from "./components/QuickOpen";
import { ResultsPane } from "./components/ResultsPane";
import { SettingsDialog } from "./components/SettingsDialog";
import { SchemaPane } from "./components/SchemaPane";
import { StatusBar } from "./components/StatusBar";
import { TitleBar } from "./components/TitleBar";
import { WorkspaceRecovery } from "./components/WorkspaceRecovery";
import {
  commandForKeyboardEvent,
  commandById,
  type WorkbenchCommandId,
} from "./data/commands";
import { getAppStatus, subscribeFileDrops } from "./lib/tauri";
import { useCenterWorkspaceFlip } from "./lib/motion";
import {
  useWorkbenchStore,
  type OperationView,
} from "./store/workbench";

const operationCommands = new Map<WorkbenchCommandId, OperationView>([
  ["sessions", "sessions"],
  ["locks", "locks"],
  ["transactions", "transactions"],
  ["roles", "roles"],
  ["wal-checkpoints", "wal"],
  ["backup-restore", "backup"],
  ["import-export", "importExport"],
  ["service-manager", "service"],
]);

function Workbench() {
  const schemaVisible = useWorkbenchStore((state) => state.schemaVisible);
  const inspectorVisible = useWorkbenchStore(
    (state) => state.inspectorVisible,
  );
  const inspectorMode = useWorkbenchStore((state) => state.inspectorMode);
  const commandPaletteOpen = useWorkbenchStore(
    (state) => state.commandPaletteOpen,
  );
  const pluginManagerOpen = useWorkbenchStore(
    (state) => state.pluginManagerOpen,
  );
  const dataSourceOpen = useWorkbenchStore((state) => state.dataSourceOpen);
  const settingsOpen = useWorkbenchStore((state) => state.settingsOpen);
  const quickOpenMode = useWorkbenchStore((state) => state.quickOpenMode);
  const recovery = useWorkbenchStore((state) => state.recovery);
  const operationsOpen = useWorkbenchStore((state) => state.operationsOpen);
  const initialize = useWorkbenchStore((state) => state.initialize);
  const toggleSchema = useWorkbenchStore((state) => state.toggleSchema);
  const toggleInspector = useWorkbenchStore((state) => state.toggleInspector);
  const setSchemaVisible = useWorkbenchStore(
    (state) => state.setSchemaVisible,
  );
  const setInspectorVisible = useWorkbenchStore(
    (state) => state.setInspectorVisible,
  );
  const setInspectorMode = useWorkbenchStore((state) => state.setInspectorMode);
  const setSidebarView = useWorkbenchStore((state) => state.setSidebarView);
  const setQuickOpenMode = useWorkbenchStore(
    (state) => state.setQuickOpenMode,
  );
  const setCommandPaletteOpen = useWorkbenchStore(
    (state) => state.setCommandPaletteOpen,
  );
  const setPluginManagerOpen = useWorkbenchStore(
    (state) => state.setPluginManagerOpen,
  );
  const setDataSourceOpen = useWorkbenchStore(
    (state) => state.setDataSourceOpen,
  );
  const setSettingsOpen = useWorkbenchStore((state) => state.setSettingsOpen);
  const setOperationsOpen = useWorkbenchStore(
    (state) => state.setOperationsOpen,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const openWorkspace = useWorkbenchStore((state) => state.openWorkspace);
  const openFile = useWorkbenchStore((state) => state.openFile);
  const openExternalFiles = useWorkbenchStore(
    (state) => state.openExternalFiles,
  );
  const createDocument = useWorkbenchStore((state) => state.createDocument);
  const saveActiveDocument = useWorkbenchStore(
    (state) => state.saveActiveDocument,
  );
  const saveActiveDocumentAs = useWorkbenchStore(
    (state) => state.saveActiveDocumentAs,
  );
  const saveAllDocuments = useWorkbenchStore(
    (state) => state.saveAllDocuments,
  );
  const formatActiveDocument = useWorkbenchStore(
    (state) => state.formatActiveDocument,
  );
  const restoreRecovery = useWorkbenchStore((state) => state.restoreRecovery);
  const discardRecovery = useWorkbenchStore((state) => state.discardRecovery);
  const runQuery = useWorkbenchStore((state) => state.runQuery);
  const runExplain = useWorkbenchStore((state) => state.runExplain);
  const cancelQuery = useWorkbenchStore((state) => state.cancelQuery);
  const openOperations = useWorkbenchStore((state) => state.openOperations);
  const centerWorkspaceRef = useCenterWorkspaceFlip(
    schemaVisible,
    inspectorVisible,
  );
  const statusQuery = useQuery({
    queryKey: ["app-status"],
    queryFn: getAppStatus,
    staleTime: Number.POSITIVE_INFINITY,
    retry: 1,
  });

  useEffect(() => {
    void initialize();
  }, [initialize]);

  const handleCommand = useCallback(
    (commandId: WorkbenchCommandId) => {
      if (commandId === "toggle-explorer") {
        toggleSchema();
        return;
      }
      if (commandId === "toggle-inspector") {
        toggleInspector();
        return;
      }
      if (commandId === "database-view") {
        setSchemaVisible(true);
        setSidebarView("database");
        return;
      }
      if (commandId === "files-view") {
        setSchemaVisible(true);
        setSidebarView("workspace");
        return;
      }
      if (commandId === "object-inspector") {
        setInspectorMode("object");
        return;
      }
      if (commandId === "ai-workbench") {
        setInspectorMode("ai");
        return;
      }
      if (commandId === "command-palette") {
        setCommandPaletteOpen(true);
        return;
      }
      if (commandId === "data-sources") {
        setDataSourceOpen(true);
        return;
      }
      if (commandId === "run-query") {
        void runQuery();
        return;
      }
      if (commandId === "explain-query") {
        void runExplain();
        return;
      }
      if (commandId === "stop-query") {
        void cancelQuery();
        return;
      }
      if (commandId === "new-query") {
        void createDocument();
        return;
      }
      if (commandId === "open-file") {
        void openFile();
        return;
      }
      if (commandId === "open-project") {
        void openWorkspace();
        return;
      }
      if (commandId === "save-all") {
        void saveAllDocuments();
        return;
      }
      if (commandId === "save-file") {
        void saveActiveDocument();
        return;
      }
      if (commandId === "save-as") {
        void saveActiveDocumentAs();
        return;
      }
      if (commandId === "format-sql") {
        formatActiveDocument();
        return;
      }
      if (commandId === "recent-files") {
        setQuickOpenMode("recent");
        return;
      }
      if (commandId === "go-to-file") {
        setQuickOpenMode("files");
        return;
      }
      if (commandId === "focus-navigation") {
        document.querySelector<HTMLElement>("[data-navigation-bar]")?.focus();
        return;
      }
      if (commandId === "settings") {
        setSettingsOpen(true);
        return;
      }
      const operation = operationCommands.get(commandId);
      if (operation) {
        void openOperations(operation);
        return;
      }

      const command = commandById.get(commandId);
      setNotice(`${command?.label ?? "命令"} · 尚未提供`);
    },
    [
      cancelQuery,
      createDocument,
      formatActiveDocument,
      openFile,
      openWorkspace,
      openOperations,
      runExplain,
      runQuery,
      saveAllDocuments,
      saveActiveDocument,
      saveActiveDocumentAs,
      setCommandPaletteOpen,
      setDataSourceOpen,
      setInspectorVisible,
      setInspectorMode,
      setQuickOpenMode,
      setSchemaVisible,
      setSidebarView,
      setSettingsOpen,
      setPluginManagerOpen,
      setNotice,
      toggleInspector,
      toggleSchema,
    ],
  );

  useEffect(() => {
    let lastShiftAt = 0;
    const handleKeyDown = (event: KeyboardEvent) => {
      const modalOwnsFocus =
        commandPaletteOpen ||
        pluginManagerOpen ||
        dataSourceOpen ||
        settingsOpen ||
        operationsOpen ||
        recovery !== null ||
        quickOpenMode !== null;
      if (event.isComposing || event.repeat || modalOwnsFocus) {
        lastShiftAt = 0;
        return;
      }
      if (
        event.key === "Shift" &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        const now = performance.now();
        if (now - lastShiftAt <= 400) {
          event.preventDefault();
          lastShiftAt = 0;
          setQuickOpenMode("global");
        } else {
          lastShiftAt = now;
        }
        return;
      }
      lastShiftAt = 0;
      const commandId = commandForKeyboardEvent(event);
      if (commandId) {
        event.preventDefault();
        handleCommand(commandId);
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [
    commandPaletteOpen,
    dataSourceOpen,
    handleCommand,
    operationsOpen,
    pluginManagerOpen,
    quickOpenMode,
    recovery,
    setQuickOpenMode,
    settingsOpen,
  ]);

  useEffect(() => {
    let dispose: (() => void) | undefined;
    void subscribeFileDrops((paths) => {
      void openExternalFiles(paths);
    }).then((unlisten) => {
      dispose = unlisten;
    });
    return () => dispose?.();
  }, [openExternalFiles]);

  return (
    <div className="app-shell">
      <TitleBar
        schemaVisible={schemaVisible}
        inspectorVisible={inspectorVisible}
        inspectorMode={inspectorMode}
        onCommand={handleCommand}
      />
      <NavigationBar />

      <main
        className={`workbench ${
          schemaVisible ? "" : "workbench--schema-hidden"
        } ${inspectorVisible ? "" : "workbench--inspector-hidden"} ${
          inspectorVisible && inspectorMode === "ai" ? "workbench--ai-pane" : ""
        }`}
      >
        <div
          className="pane-slot pane-slot--schema island"
          aria-hidden={!schemaVisible}
        >
          {schemaVisible && <SchemaPane />}
        </div>

        <div className="center-workspace island" ref={centerWorkspaceRef}>
          <EditorPane />
          <ResultsPane />
        </div>

        <div
          className="pane-slot pane-slot--inspector island"
          aria-hidden={!inspectorVisible}
        >
          {inspectorVisible &&
            (inspectorMode === "ai" ? <AiWorkbench /> : <ObjectInspector />)}
        </div>
      </main>

      <StatusBar status={statusQuery.data} loading={statusQuery.isLoading} />
      <CommandPalette
        open={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        onCommand={handleCommand}
      />
      <QuickOpen onCommand={handleCommand} />
      <ConnectorManager
        open={pluginManagerOpen}
        onClose={() => setPluginManagerOpen(false)}
      />
      <DataSourceDialog
        open={dataSourceOpen}
        onClose={() => setDataSourceOpen(false)}
        onOpenPluginManager={() => setPluginManagerOpen(true)}
      />
      <OperationsPanel
        open={operationsOpen}
        onClose={() => setOperationsOpen(false)}
      />
      <SettingsDialog
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
      <WorkspaceRecovery
        open={recovery !== null}
        onRestore={() => void restoreRecovery()}
        onDiscard={() => void discardRecovery()}
      />
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="*" element={<Workbench />} />
    </Routes>
  );
}
