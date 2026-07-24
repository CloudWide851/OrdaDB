import Editor, { type BeforeMount } from "@monaco-editor/react";
import {
  ChevronDown,
  Clock3,
  History,
  MoreHorizontal,
  Play,
  Plus,
  WandSparkles,
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
          查询 01
        </button>
        <IconAction
          label="新建查询"
          className="query-add"
          icon={<Plus size={17} aria-hidden="true" />}
        />
        <span className="query-tabs-spacer" />
        <div className="connection-selector">
          <span className="connection-dot" aria-hidden="true" />
          ordadb_local
          <ChevronDown size={14} aria-hidden="true" />
        </div>
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
        <span className="toolbar-divider" aria-hidden="true" />
        <IconAction
          label="格式化 SQL"
          icon={<WandSparkles size={17} aria-hidden="true" />}
        />
        <IconAction
          label="查询历史"
          icon={<History size={17} aria-hidden="true" />}
        />
        <IconAction
          label="执行计划"
          icon={<Clock3 size={17} aria-hidden="true" />}
        />
        <span className="toolbar-spacer" />
        <span className="preview-badge">PREVIEW</span>
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
