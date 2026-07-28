import Editor, {
  type BeforeMount,
  type Monaco,
  type OnMount,
} from "@monaco-editor/react";
import {
  AlignLeft,
  Check,
  ChevronDown,
  GitBranch,
  ListTree,
  Play,
  Plus,
  RotateCcw,
  Save,
  Square,
  X,
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
  const settings = useWorkbenchStore((state) => state.settings);
  const workspace = useWorkbenchStore((state) => state.workspace);
  const documents = useWorkbenchStore((state) => state.documents);
  const activeDocumentPath = useWorkbenchStore(
    (state) => state.activeDocumentPath,
  );
  const openWorkspace = useWorkbenchStore((state) => state.openWorkspace);
  const createDocument = useWorkbenchStore((state) => state.createDocument);
  const activateDocument = useWorkbenchStore(
    (state) => state.activateDocument,
  );
  const closeDocument = useWorkbenchStore((state) => state.closeDocument);
  const reloadActiveDocument = useWorkbenchStore(
    (state) => state.reloadActiveDocument,
  );
  const saveActiveDocument = useWorkbenchStore(
    (state) => state.saveActiveDocument,
  );
  const activeDocument = documents.find(
    (document) => document.path === activeDocumentPath,
  );
  const dialect = useWorkbenchStore((state) => state.dialect);
  const setDialect = useWorkbenchStore((state) => state.setDialect);
  const queryState = useWorkbenchStore((state) => state.queryState);
  const runQuery = useWorkbenchStore((state) => state.runQuery);
  const cancelQuery = useWorkbenchStore((state) => state.cancelQuery);
  const runExplain = useWorkbenchStore((state) => state.runExplain);
  const beginTransaction = useWorkbenchStore(
    (state) => state.beginTransaction,
  );
  const commitTransaction = useWorkbenchStore(
    (state) => state.commitTransaction,
  );
  const rollbackTransaction = useWorkbenchStore(
    (state) => state.rollbackTransaction,
  );
  const transactionActive = useWorkbenchStore(
    (state) => state.transactionActive,
  );
  const connection = useWorkbenchStore((state) => state.connection);
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
        {documents.map((document) => {
          const active = document.path === activeDocumentPath;
          return (
            <div
              className={`query-tab-wrap ${active ? "query-tab-wrap--active" : ""}`}
              key={document.path}
            >
              <button
                type="button"
                className={`query-tab ${active ? "query-tab--active" : ""}`}
                role="tab"
                aria-selected={active}
                onClick={() => activateDocument(document.path)}
              >
                {(document.dirty || document.conflict) && (
                  <span
                    className={`query-dot ${
                      document.conflict ? "query-dot--conflict" : ""
                    }`}
                    aria-label={document.conflict ? "外部冲突" : "未保存"}
                  />
                )}
                {document.name}
              </button>
              <IconAction
                label={`关闭 ${document.name}`}
                className="query-close"
                icon={<X size={12} aria-hidden="true" />}
                onClick={() => {
                  if (
                    !document.dirty ||
                    window.confirm(`${document.name} 尚未保存，仍要关闭吗？`)
                  ) {
                    void closeDocument(document.path);
                  }
                }}
              />
            </div>
          );
        })}
        <IconAction
          label={workspace ? "新建 SQL 文件" : "打开 SQL 项目"}
          className="query-add"
          icon={<Plus size={17} aria-hidden="true" />}
          onClick={() =>
            workspace ? void createDocument() : void openWorkspace()
          }
        />
        <span className="query-tabs-spacer" />
        <label className="dialect-selector">
          <span className="connection-dot" aria-hidden="true" />
          <select
            aria-label="SQL 方言"
            aria-describedby="dialect-tooltip"
            value={dialect}
            disabled={connection !== null && connection.mode !== "preview"}
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
          disabled={queryState === "running" || !connection || !activeDocumentPath}
          onClick={() => void runQuery()}
        >
          <Play size={15} fill="currentColor" aria-hidden="true" />
          {queryState === "running" ? "运行中" : "运行"}
          <kbd>Ctrl↵</kbd>
        </button>
        <IconAction
          label="停止查询"
          disabled={queryState !== "running"}
          icon={<Square size={14} fill="currentColor" aria-hidden="true" />}
          onClick={() => void cancelQuery()}
        />
        <span className="toolbar-divider" aria-hidden="true" />
        <IconAction
          label="格式化 SQL"
          disabled={!activeDocumentPath}
          icon={<AlignLeft size={17} aria-hidden="true" />}
          onClick={() => {
            setSql(formatSqlForDialect(sql, dialectDescriptor));
            setNotice(`格式化 SQL · ${dialectDescriptor.label}`);
          }}
        />
        <IconAction
          label="保存 SQL 文件"
          disabled={!activeDocumentPath}
          icon={<Save size={16} aria-hidden="true" />}
          onClick={() => void saveActiveDocument()}
        />
        {activeDocument?.conflict && (
          <div className="conflict-actions" role="alert">
            <span>文件已在外部修改</span>
            <button
              type="button"
              onClick={() => void reloadActiveDocument()}
            >
              重新加载
            </button>
            <button
              type="button"
              onClick={() => void saveActiveDocument(true)}
            >
              覆盖保存
            </button>
          </div>
        )}
        <IconAction
          label="执行计划"
          icon={<ListTree size={17} aria-hidden="true" />}
          disabled={!connection?.capabilities.explain}
          onClick={() => void runExplain()}
        />
        {!transactionActive ? (
          <button
            className="transaction-mode"
            type="button"
            disabled={!connection?.capabilities.transactions}
            onClick={() => void beginTransaction()}
          >
            <GitBranch size={14} aria-hidden="true" />
            开始事务
          </button>
        ) : (
          <div className="transaction-actions" aria-label="活动事务">
            <button
              className="transaction-mode transaction-mode--active"
              type="button"
              onClick={() => void commitTransaction()}
            >
              <Check size={14} aria-hidden="true" />
              提交
            </button>
            <IconAction
              label="回滚事务"
              icon={<RotateCcw size={15} aria-hidden="true" />}
              onClick={() => void rollbackTransaction()}
            />
          </div>
        )}
      </div>

      <div className="monaco-shell">
        {activeDocumentPath ? (
          <Editor
            beforeMount={configureMonaco}
            onMount={handleEditorMount}
            height="100%"
            language="sql"
            path={`${activeDocumentPath}.${dialect}`}
            theme="ordadb-light"
            value={sql}
            onChange={(value) => setSql(value ?? "")}
            loading={<span className="editor-loading">正在加载 SQL 编辑器</span>}
            options={{
              ariaLabel: "SQL 编辑器",
              fontFamily:
                '"Cascadia Code", "SFMono-Regular", Consolas, monospace',
              fontSize: settings.editorFontSize,
              lineHeight: settings.editorFontSize + 7,
              minimap: { enabled: false },
              padding: { top: 12, bottom: 12 },
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
        ) : (
          <div className="editor-empty-state">
            <button type="button" onClick={() => void openWorkspace()}>
              打开 SQL 项目
            </button>
            <span>或</span>
            <button type="button" onClick={() => void createDocument()}>
              新建 SQL 文件
            </button>
          </div>
        )}
      </div>
    </section>
  );
}
