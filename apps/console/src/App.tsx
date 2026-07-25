import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useLayoutEffect, useRef } from "react";
import { Route, Routes } from "react-router-dom";
import { CommandPalette } from "./components/CommandPalette";
import { EditorPane } from "./components/EditorPane";
import { ObjectInspector } from "./components/ObjectInspector";
import { ResultsPane } from "./components/ResultsPane";
import { SchemaPane } from "./components/SchemaPane";
import { StatusBar } from "./components/StatusBar";
import { TitleBar } from "./components/TitleBar";
import {
  commandById,
  type WorkbenchCommandId,
} from "./data/commands";
import { getAppStatus } from "./lib/tauri";
import { useWorkbenchStore } from "./store/workbench";

const PANEL_MOTION_MS = 180;
const PANEL_EASING = "cubic-bezier(0.16, 1, 0.3, 1)";

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
  const toggleSchema = useWorkbenchStore((state) => state.toggleSchema);
  const toggleInspector = useWorkbenchStore((state) => state.toggleInspector);
  const setCommandPaletteOpen = useWorkbenchStore(
    (state) => state.setCommandPaletteOpen,
  );
  const setActiveResultTab = useWorkbenchStore(
    (state) => state.setActiveResultTab,
  );
  const setSql = useWorkbenchStore((state) => state.setSql);
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const runPreviewQuery = useWorkbenchStore((state) => state.runPreviewQuery);
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
      if (commandId === "run-query") {
        void runPreviewQuery();
        return;
      }
      if (commandId === "explain-query") {
        setActiveResultTab("plan");
        setNotice("执行计划 · 预览数据");
        return;
      }
      if (commandId === "new-query") {
        setSql("SELECT *\nFROM public.documents\nORDER BY updated_at DESC\nLIMIT 100;");
        setNotice("新查询已创建 · 本地草稿");
        return;
      }

      const command = commandById.get(commandId);
      setNotice(`${command?.label ?? "命令"} · 预览入口`);
    },
    [
      runPreviewQuery,
      setActiveResultTab,
      setCommandPaletteOpen,
      setNotice,
      setSql,
      toggleInspector,
      toggleSchema,
    ],
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        void runPreviewQuery();
      } else if (
        (event.metaKey || event.ctrlKey) &&
        event.shiftKey &&
        event.key.toLowerCase() === "p"
      ) {
        event.preventDefault();
        setCommandPaletteOpen(true);
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
    runPreviewQuery,
    setCommandPaletteOpen,
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
