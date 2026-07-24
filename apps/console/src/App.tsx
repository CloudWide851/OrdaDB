import { useQuery } from "@tanstack/react-query";
import {
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
} from "lucide-react";
import { useEffect } from "react";
import { Route, Routes } from "react-router-dom";
import { AssistantPane } from "./components/AssistantPane";
import { EditorPane } from "./components/EditorPane";
import { IconAction } from "./components/IconAction";
import { ResultsPane } from "./components/ResultsPane";
import { SchemaPane } from "./components/SchemaPane";
import { TitleBar } from "./components/TitleBar";
import { getAppStatus } from "./lib/tauri";
import { useWorkbenchStore } from "./store/workbench";

function Workbench() {
  const schemaVisible = useWorkbenchStore((state) => state.schemaVisible);
  const assistantVisible = useWorkbenchStore((state) => state.assistantVisible);
  const toggleSchema = useWorkbenchStore((state) => state.toggleSchema);
  const toggleAssistant = useWorkbenchStore((state) => state.toggleAssistant);
  const runPreviewQuery = useWorkbenchStore((state) => state.runPreviewQuery);
  const statusQuery = useQuery({
    queryKey: ["app-status"],
    queryFn: getAppStatus,
    staleTime: Number.POSITIVE_INFINITY,
    retry: 1,
  });

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        void runPreviewQuery();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [runPreviewQuery]);

  return (
    <div className="app-shell">
      <TitleBar status={statusQuery.data} loading={statusQuery.isLoading} />

      <div className="workspace-toolbar" aria-label="工作区布局">
        <IconAction
          label={schemaVisible ? "隐藏 Schema" : "显示 Schema"}
          icon={
            schemaVisible ? (
              <PanelLeftClose size={17} aria-hidden="true" />
            ) : (
              <PanelLeftOpen size={17} aria-hidden="true" />
            )
          }
          onClick={toggleSchema}
        />
        <span className="workspace-title">documents / 混合检索</span>
        <span className="workspace-toolbar-spacer" />
        <span className="autosave-state">已自动保存</span>
        <IconAction
          label={assistantVisible ? "隐藏查询助手" : "显示查询助手"}
          icon={
            assistantVisible ? (
              <PanelRightClose size={17} aria-hidden="true" />
            ) : (
              <PanelRightOpen size={17} aria-hidden="true" />
            )
          }
          onClick={toggleAssistant}
        />
      </div>

      <main
        className={`workbench ${
          schemaVisible ? "" : "workbench--schema-hidden"
        } ${assistantVisible ? "" : "workbench--assistant-hidden"}`}
      >
        <div className="pane-slot pane-slot--schema" aria-hidden={!schemaVisible}>
          {schemaVisible && <SchemaPane />}
        </div>

        <div className="center-workspace">
          <EditorPane />
          <ResultsPane />
        </div>

        <div
          className="pane-slot pane-slot--assistant"
          aria-hidden={!assistantVisible}
        >
          {assistantVisible && <AssistantPane />}
        </div>
      </main>
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
