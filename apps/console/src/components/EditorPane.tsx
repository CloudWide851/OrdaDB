import Editor, {
  type BeforeMount,
  type Monaco,
  type OnMount,
} from "@monaco-editor/react";
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
import { useCallback, useEffect, useRef } from "react";
import {
  formatSqlForDialect,
  getSqlDialect,
  sqlDialects,
  type SqlDialectDescriptor,
} from "../data/dialects";
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

function registerDialectCompletion(
  monaco: Monaco,
  dialect: SqlDialectDescriptor,
) {
  const completion = monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: ["$", "@", "?", "`", "["],
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };
      const keywordSuggestions = dialect.keywords.map((keyword) => ({
        label: keyword,
        kind: monaco.languages.CompletionItemKind.Keyword,
        insertText: keyword,
        detail: `${dialect.label} 关键字`,
        range,
      }));

      return {
        suggestions: [
          ...keywordSuggestions,
          {
            label: dialect.parameterExample,
            kind: monaco.languages.CompletionItemKind.Variable,
            insertText: dialect.parameterExample,
            detail: `${dialect.label} 位置参数`,
            range,
          },
          {
            label: dialect.paginationExample,
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: dialect.paginationExample,
            detail: `${dialect.label} 分页`,
            range,
          },
        ],
      };
    },
  });
  const formatting = monaco.languages.registerDocumentFormattingEditProvider(
    "sql",
    {
      provideDocumentFormattingEdits(model) {
        return [
          {
            range: model.getFullModelRange(),
            text: formatSqlForDialect(model.getValue(), dialect),
          },
        ];
      },
    },
  );

  return {
    dispose() {
      completion.dispose();
      formatting.dispose();
    },
  };
}

export function EditorPane() {
  const monacoRef = useRef<Monaco | null>(null);
  const completionRef = useRef<{ dispose: () => void } | null>(null);
  const sql = useWorkbenchStore((state) => state.sql);
  const setSql = useWorkbenchStore((state) => state.setSql);
  const dialect = useWorkbenchStore((state) => state.dialect);
  const setDialect = useWorkbenchStore((state) => state.setDialect);
  const queryState = useWorkbenchStore((state) => state.queryState);
  const runPreviewQuery = useWorkbenchStore((state) => state.runPreviewQuery);
  const setActiveResultTab = useWorkbenchStore(
    (state) => state.setActiveResultTab,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const dialectDescriptor = getSqlDialect(dialect);

  const installCompletion = useCallback(
    (monaco: Monaco) => {
      completionRef.current?.dispose();
      completionRef.current = registerDialectCompletion(
        monaco,
        dialectDescriptor,
      );
    },
    [dialectDescriptor],
  );

  const handleEditorMount: OnMount = (_editor, monaco) => {
    monacoRef.current = monaco;
    installCompletion(monaco);
  };

  useEffect(() => {
    if (monacoRef.current) {
      installCompletion(monacoRef.current);
    }

    return () => {
      completionRef.current?.dispose();
      completionRef.current = null;
    };
  }, [installCompletion]);

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
        <label className="dialect-selector">
          <span className="connection-dot" aria-hidden="true" />
          <select
            aria-label="SQL 方言"
            aria-describedby="dialect-tooltip"
            value={dialect}
            onChange={(event) => {
              const nextDialect = sqlDialects.find(
                (candidate) => candidate.id === event.target.value,
              );
              if (nextDialect) {
                setDialect(nextDialect.id);
              }
            }}
          >
            {sqlDialects.map((candidate) => (
              <option key={candidate.id} value={candidate.id}>
                {candidate.label}
              </option>
            ))}
          </select>
          <ChevronDown className="dialect-chevron" size={14} aria-hidden="true" />
          <span
            className="dialect-tooltip"
            id="dialect-tooltip"
            role="tooltip"
          >
            SQL 方言 · 参数 {dialectDescriptor.parameterExample}
          </span>
        </label>
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
          onClick={() => {
            setSql(formatSqlForDialect(sql, dialectDescriptor));
            setNotice(`格式化 SQL · ${dialectDescriptor.label} · 预览入口`);
          }}
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
        <span
          className="dialect-parameter"
          title={`${dialectDescriptor.label} 位置参数`}
        >
          参数 {dialectDescriptor.parameterExample}
        </span>
        <span className="preview-badge">预览</span>
        <IconAction
          label="更多查询操作"
          icon={<MoreHorizontal size={18} aria-hidden="true" />}
        />
      </div>

      <div className="monaco-shell">
        <Editor
          beforeMount={configureMonaco}
          onMount={handleEditorMount}
          height="100%"
          language="sql"
          path={`query_01.${dialect}.sql`}
          theme="ordadb-light"
          value={sql}
          onChange={(value) => setSql(value ?? "")}
          loading={<span className="editor-loading">正在加载 SQL 编辑器</span>}
          options={{
            ariaLabel: "SQL 编辑器",
            fontFamily:
              '"Cascadia Code", "SFMono-Regular", Consolas, monospace',
            fontSize: 14,
            lineHeight: 22,
            minimap: { enabled: false },
            padding: { top: 16, bottom: 16 },
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
