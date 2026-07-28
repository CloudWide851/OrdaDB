import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useLayoutEffect, useRef } from "react";
import { Route, Routes } from "react-router-dom";
import { CommandPalette } from "./components/CommandPalette";
import { ConnectorManager } from "./components/ConnectorManager";
import { DataSourceDialog } from "./components/DataSourceDialog";
import { EditorPane } from "./components/EditorPane";
import { ObjectInspector } from "./components/ObjectInspector";
import { OperationsPanel } from "./components/OperationsPanel";
import { ResultsPane } from "./components/ResultsPane";
import { SettingsDialog } from "./components/SettingsDialog";
import { SchemaPane } from "./components/SchemaPane";
import { StatusBar } from "./components/StatusBar";
import { TitleBar } from "./components/TitleBar";
import { WorkspaceRecovery } from "./components/WorkspaceRecovery";
import {
  commandById,
  type WorkbenchCommandId,
} from "./data/commands";
import { getAppStatus } from "./lib/tauri";
import {
  useWorkbenchStore,
  type OperationView,
} from "./store/workbench";

const PANEL_MOTION_MS = 180;
const PANEL_EASING = "cubic-bezier(0.16, 1, 0.3, 1)";
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

function useCenterWorkspaceFlip(
  schemaVisible: boolean,
  inspectorVisible: boolean,
) {
  const centerRef = useRef<HTMLDivElement>(null);
  const previousRectRef = useRef<DOMRect | null>(null);

  useLayoutEffect(() => {
    const element = centerRef.current;
    if (!element) return;

    const nextRect = element.getBoundingClientRect();
    const previousRect = previousRectRef.current;
    previousRectRef.current = nextRect;

    const reducedMotion =
      window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
    if (
      !previousRect ||
      reducedMotion ||
      typeof element.animate !== "function" ||
      nextRect.width === 0
    ) {
      return;
    }

    const deltaX = previousRect.left - nextRect.left;
    const scaleX = previousRect.width / nextRect.width;
    if (Math.abs(deltaX) < 0.5 && Math.abs(scaleX - 1) < 0.002) return;

    const animation = element.animate(
      [
        {
          transform: `translateX(${deltaX}px) scaleX(${scaleX})`,
          transformOrigin: "left center",
        },
        {
          transform: "translateX(0) scaleX(1)",
          transformOrigin: "left center",
        },
      ],
      {
        duration: PANEL_MOTION_MS,
        easing: PANEL_EASING,
      },
    );

    return () => animation.cancel();
  }, [inspectorVisible, schemaVisible]);

  return centerRef;
}

function Workbench() {
  const schemaVisible = useWorkbenchStore((state) => state.schemaVisible);
  const inspectorVisible = useWorkbenchStore(
    (state) => state.inspectorVisible,
  );
  const commandPaletteOpen = useWorkbenchStore(
    (state) => state.commandPaletteOpen,
  );
  const pluginManagerOpen = useWorkbenchStore(
    (state) => state.pluginManagerOpen,
  );
  const dataSourceOpen = useWorkbenchStore((state) => state.dataSourceOpen);
  const settingsOpen = useWorkbenchStore((state) => state.settingsOpen);
  const recovery = useWorkbenchStore((state) => state.recovery);
  const operationsOpen = useWorkbenchStore((state) => state.operationsOpen);
  const initialize = useWorkbenchStore((state) => state.initialize);
  const toggleSchema = useWorkbenchStore((state) => state.toggleSchema);
  const toggleInspector = useWorkbenchStore((state) => state.toggleInspector);
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
  const createDocument = useWorkbenchStore((state) => state.createDocument);
  const saveAllDocuments = useWorkbenchStore(
    (state) => state.saveAllDocuments,
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
      if (commandId === "open-project") {
        void openWorkspace();
        return;
      }
      if (commandId === "save-all") {
        void saveAllDocuments();
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
      openWorkspace,
      openOperations,
      runExplain,
      runQuery,
      saveAllDocuments,
      setCommandPaletteOpen,
      setDataSourceOpen,
      setSettingsOpen,
      setPluginManagerOpen,
      setNotice,
      toggleInspector,
      toggleSchema,
    ],
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        void runQuery();
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.shiftKey &&
        event.key.toLowerCase() === "p"
      ) {
        event.preventDefault();
        setCommandPaletteOpen(true);
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.altKey &&
        event.shiftKey &&
        event.key.toLowerCase() === "s"
      ) {
        event.preventDefault();
        setPluginManagerOpen(true);
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.key.toLowerCase() === "o"
      ) {
        event.preventDefault();
        void openWorkspace();
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.altKey &&
        event.key.toLowerCase() === "s"
      ) {
        event.preventDefault();
        setSettingsOpen(true);
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.key.toLowerCase() === "s"
      ) {
        event.preventDefault();
        void saveAllDocuments();
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.key.toLowerCase() === "n"
      ) {
        event.preventDefault();
        void createDocument();
      } else if (event.altKey && event.key === "1") {
        event.preventDefault();
        toggleSchema();
      } else if (event.altKey && event.key === "2") {
        event.preventDefault();
        toggleInspector();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [
    runQuery,
    createDocument,
    openWorkspace,
    saveAllDocuments,
    setCommandPaletteOpen,
    setPluginManagerOpen,
    setSettingsOpen,
    toggleInspector,
    toggleSchema,
  ]);

  return (
    <div className="app-shell">
      <TitleBar
        schemaVisible={schemaVisible}
        inspectorVisible={inspectorVisible}
        onCommand={handleCommand}
      />

      <main
        className={`workbench ${
          schemaVisible ? "" : "workbench--schema-hidden"
        } ${inspectorVisible ? "" : "workbench--inspector-hidden"}`}
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
          {inspectorVisible && <ObjectInspector />}
        </div>
      </main>

      <StatusBar status={statusQuery.data} loading={statusQuery.isLoading} />
      <CommandPalette
        open={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        onCommand={handleCommand}
      />
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
