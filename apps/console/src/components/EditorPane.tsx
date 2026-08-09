import Editor, {
  type BeforeMount,
  type Monaco,
  type OnMount,
} from "@monaco-editor/react";
import {
  AlignLeft,
  Braces,
  Check,
  ChevronDown,
  FileOutput,
  FilePenLine,
  GitBranch,
  ListTree,
  Play,
  Plus,
  RotateCcw,
  Save,
  Square,
  TriangleAlert,
  X,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
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
  monaco.editor.defineTheme("ordadb-dark", {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "keyword.sql", foreground: "78A9D1", fontStyle: "bold" },
      { token: "string.sql", foreground: "D7A678" },
      { token: "number.sql", foreground: "BE8FD1" },
      { token: "comment.sql", foreground: "8B9AA5", fontStyle: "italic" },
    ],
    colors: {
      "editor.background": "#172027",
      "editor.foreground": "#D9E2E8",
      "editorLineNumber.foreground": "#6F808C",
      "editorLineNumber.activeForeground": "#78A9D1",
      "editor.lineHighlightBackground": "#202C34",
      "editorCursor.foreground": "#78A9D1",
      "editor.selectionBackground": "#34546A",
      "editorIndentGuide.background1": "#2B3942",
      "editorIndentGuide.activeBackground1": "#587181",
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
  const editorBlurRef = useRef<{ dispose: () => void } | null>(null);
  const [systemDark, setSystemDark] = useState(
    () =>
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-color-scheme: dark)").matches,
  );
  const sql = useWorkbenchStore((state) => state.sql);
  const setSql = useWorkbenchStore((state) => state.setSql);
  const settings = useWorkbenchStore((state) => state.settings);
  const documents = useWorkbenchStore((state) => state.documents);
  const activeDocumentPath = useWorkbenchStore(
    (state) => state.activeDocumentPath,
  );
  const openWorkspace = useWorkbenchStore((state) => state.openWorkspace);
  const openFile = useWorkbenchStore((state) => state.openFile);
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
  const saveActiveDocumentAs = useWorkbenchStore(
    (state) => state.saveActiveDocumentAs,
  );
  const saveActiveDocumentOnFocusChange = useWorkbenchStore(
    (state) => state.saveActiveDocumentOnFocusChange,
  );
  const formatActiveDocument = useWorkbenchStore(
    (state) => state.formatActiveDocument,
  );
  const focusSaveRef = useRef(saveActiveDocumentOnFocusChange);
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
  const sqlMode =
    connection?.connectorKind !== "document" &&
    connection?.connectorKind !== "keyValue";
  const editorLanguage =
    connection?.connectorKind === "document"
      ? "json"
      : connection?.connectorKind === "keyValue"
        ? "plaintext"
        : "sql";
  const editorLabel = sqlMode ? "SQL 编辑器" : "命令编辑器";
  const dialectDescriptor = getSqlDialect(dialect);
  const editorTheme =
    settings.appearance.theme === "dark" ||
    (settings.appearance.theme === "system" && systemDark)
      ? "ordadb-dark"
      : "ordadb-light";

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const update = () => setSystemDark(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);

  useEffect(() => {
    focusSaveRef.current = saveActiveDocumentOnFocusChange;
  }, [saveActiveDocumentOnFocusChange]);

  const installCompletion = useCallback(
    (monaco: Monaco) => {
      completionRef.current?.dispose();
      completionRef.current = null;
      if (!sqlMode) return;
      completionRef.current = registerDialectCompletion(
        monaco,
        dialectDescriptor,
      );
    },
    [dialectDescriptor, sqlMode],
  );

  const handleEditorMount: OnMount = (editor, monaco) => {
    monacoRef.current = monaco;
    installCompletion(monaco);
    editorBlurRef.current?.dispose();
    editorBlurRef.current = editor.onDidBlurEditorText(() => {
      void focusSaveRef.current().catch(() => undefined);
    });
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

  useEffect(
    () => () => {
      editorBlurRef.current?.dispose();
      editorBlurRef.current = null;
    },
    [],
  );

  return (
    <section className="editor-pane" aria-label={editorLabel}>
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
                  document.conflict ? (
                    <TriangleAlert
                      className="query-document-state query-document-state--conflict"
                      size={12}
                      aria-label="外部冲突"
                    />
                  ) : (
                    <FilePenLine
                      className="query-document-state"
                      size={12}
                      aria-label="未保存"
                    />
                  )
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
                    !settings.files.confirmDirtyClose ||
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
          label="新建 SQL 文件"
          className="query-add"
          icon={<Plus size={17} aria-hidden="true" />}
          onClick={() => void createDocument()}
        />
        <span className="query-tabs-spacer" />
        {sqlMode && <label className="dialect-selector">
          <Braces size={13} aria-hidden="true" />
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
        </label>}
      </div>

      <div className="editor-toolbar">
        <button
          className="run-query"
          type="button"
          disabled={queryState === "running" || !connection || !activeDocumentPath}
          onClick={() => void runQuery()}
        >
          <Play size={15} fill="currentColor" aria-hidden="true" />
          {queryState === "running"
            ? "运行中"
            : sqlMode
              ? "运行查询"
              : "运行命令"}
          <kbd>Ctrl↵</kbd>
        </button>
        <IconAction
          label="停止命令"
          disabled={queryState !== "running"}
          icon={<Square size={14} fill="currentColor" aria-hidden="true" />}
          onClick={() => void cancelQuery()}
        />
        <span className="toolbar-divider" aria-hidden="true" />
        {sqlMode && (
          <IconAction
            label="格式化 SQL"
            disabled={!activeDocumentPath}
            icon={<AlignLeft size={17} aria-hidden="true" />}
            onClick={formatActiveDocument}
          />
        )}
        <IconAction
          label="保存 SQL 文件"
          disabled={!activeDocumentPath}
          icon={<Save size={16} aria-hidden="true" />}
          onClick={() => void saveActiveDocument()}
        />
        <IconAction
          label="SQL 文件另存为"
          disabled={!activeDocumentPath}
          icon={<FileOutput size={16} aria-hidden="true" />}
          onClick={() => void saveActiveDocumentAs()}
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
            language={editorLanguage}
            path={
              encodeURIComponent(activeDocumentPath) +
              "." +
              editorLanguage +
              "." +
              dialect
            }
            theme={editorTheme}
            value={sql}
            onChange={(value) => setSql(value ?? "")}
            loading={<span className="editor-loading">正在加载{editorLabel}</span>}
            options={{
              ariaLabel: editorLabel,
              fontFamily: settings.editor.fontFamily,
              fontSize: settings.editor.fontSize,
              lineHeight: settings.editor.fontSize + 7,
              tabSize: settings.editor.tabSize,
              wordWrap: settings.editor.wordWrap,
              minimap: { enabled: settings.editor.minimap },
              padding: { top: 12, bottom: 12 },
              scrollBeyondLastLine: false,
              smoothScrolling: true,
              renderLineHighlight: "all",
              automaticLayout: true,
              cursorBlinking: "smooth",
              overviewRulerBorder: false,
              hideCursorInOverviewRuler: true,
              stickyScroll: { enabled: false },
            }}
          />
        ) : (
          <div className="editor-empty-state">
            <button type="button" onClick={() => void createDocument()}>
              新建 SQL
            </button>
            <span>或</span>
            <button type="button" onClick={() => void openFile()}>
              打开文件
            </button>
            <span>或</span>
            <button type="button" onClick={() => void openWorkspace()}>
              打开 SQL 项目
            </button>
          </div>
        )}
      </div>
    </section>
  );
}
