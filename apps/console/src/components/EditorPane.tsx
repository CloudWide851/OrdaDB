import Editor, { type BeforeMount } from "@monaco-editor/react";
import {
  AlignLeft,
  ChevronDown,
  History,
  ListTree,
  MoreHorizontal,
  Play,
  Plus,
  Square,
} from "lucide-react";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

const configureMonaco: BeforeMount = (monaco) => {
  monaco.editor.defineTheme("ordadb-light", {
    base: "vs",
    inherit: true,
    rules: [
      { token: "keyword.sql", foreground: "416E95", fontStyle: "bold" },
      { token: "string.sql", foreground: "8A5B38" },
      { token: "number.sql", foreground: "78558C" },
      { token: "comment.sql", foreground: "71818D", fontStyle: "italic" },
    ],
    colors: {
      "editor.background": "#F9FBFC",
      "editor.foreground": "#273946",
      "editorLineNumber.foreground": "#9AA8B1",
      "editorLineNumber.activeForeground": "#416E95",
      "editor.lineHighlightBackground": "#EEF4F7",
      "editorCursor.foreground": "#416E95",
      "editor.selectionBackground": "#CFE0EB",
      "editorIndentGuide.background1": "#DFE7EC",
      "editorIndentGuide.activeBackground1": "#AABEC9",
    },
  });
};

export function EditorPane() {
  const sql = useWorkbenchStore((state) => state.sql);
  const setSql = useWorkbenchStore((state) => state.setSql);
  const queryState = useWorkbenchStore((state) => state.queryState);
  const runPreviewQuery = useWorkbenchStore((state) => state.runPreviewQuery);
  const setActiveResultTab = useWorkbenchStore(
    (state) => state.setActiveResultTab,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);

  return (
    <section className="editor-pane" aria-label="SQL 编辑器">
      <div className="query-tabs" role="tablist" aria-label="查询标签">
        <button
          type="button"
          className="query-tab query-tab--active"
          role="tab"
          aria-selected="true"
        >
          <span className="query-dot" aria-hidden="true" />
          query_01.sql
        </button>
        <button
          type="button"
          className="query-tab"
          role="tab"
          aria-selected="false"
        >
          scratch_02.sql
        </button>
        <IconAction
          label="新建查询"
          className="query-add"
          icon={<Plus size={17} aria-hidden="true" />}
        />
        <span className="query-tabs-spacer" />
        <button className="connection-selector" type="button">
          <span className="connection-dot" aria-hidden="true" />
          OrdaDB Local
          <ChevronDown size={14} aria-hidden="true" />
        </button>
      </div>

      <div className="editor-toolbar">
        <button
          className="run-query"
          type="button"
          disabled={queryState === "running"}
          onClick={() => void runPreviewQuery()}
        >
          <Play size={15} fill="currentColor" aria-hidden="true" />
          {queryState === "running" ? "运行中" : "运行"}
          <kbd>Ctrl↵</kbd>
        </button>
        <IconAction
          label="停止查询"
          disabled={queryState !== "running"}
          icon={<Square size={14} fill="currentColor" aria-hidden="true" />}
          onClick={() => setNotice("停止查询 · 预览入口")}
        />
        <span className="toolbar-divider" aria-hidden="true" />
        <IconAction
          label="格式化 SQL"
          icon={<AlignLeft size={17} aria-hidden="true" />}
          onClick={() => setNotice("格式化 SQL · 预览入口")}
        />
        <IconAction
          label="查询历史"
          icon={<History size={17} aria-hidden="true" />}
          onClick={() => setNotice("SQL 历史 · 预览入口")}
        />
        <IconAction
          label="执行计划"
          icon={<ListTree size={17} aria-hidden="true" />}
          onClick={() => {
            setActiveResultTab("plan");
            setNotice("执行计划 · 预览数据");
          }}
        />
        <button
          className="transaction-mode"
          type="button"
          onClick={() => setNotice("自动提交 · 预览模式")}
        >
          自动提交
          <ChevronDown size={14} aria-hidden="true" />
        </button>
        <span className="toolbar-spacer" />
        <span className="preview-badge">预览</span>
        <IconAction
          label="更多查询操作"
          icon={<MoreHorizontal size={18} aria-hidden="true" />}
        />
      </div>

      <div className="monaco-shell">
        <Editor
          beforeMount={configureMonaco}
          height="100%"
          language="sql"
          theme="ordadb-light"
          value={sql}
          onChange={(value) => setSql(value ?? "")}
          loading={<span className="editor-loading">正在加载 SQL 编辑器</span>}
          options={{
            ariaLabel: "SQL 编辑器",
            fontFamily:
              '"Cascadia Code", "SFMono-Regular", Consolas, monospace',
            fontSize: 15,
            lineHeight: 24,
            minimap: { enabled: false },
            padding: { top: 22, bottom: 22 },
            scrollBeyondLastLine: false,
            smoothScrolling: true,
            renderLineHighlight: "all",
            wordWrap: "on",
            automaticLayout: true,
            tabSize: 2,
            cursorBlinking: "smooth",
            overviewRulerBorder: false,
            hideCursorInOverviewRuler: true,
            stickyScroll: { enabled: false },
          }}
        />
      </div>
    </section>
  );
}
