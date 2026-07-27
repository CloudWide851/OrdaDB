import {
  Braces,
  CheckCircle2,
  Download,
  FileText,
  ListTree,
  Rows3,
  Search,
} from "lucide-react";
import type {
  DbmsError,
  DbmsQueryColumn,
} from "../lib/dbmsClient";
import { useWorkbenchStore } from "../store/workbench";
import type { QueryState, ResultTab } from "../types";
import { IconAction } from "./IconAction";

const tabs: Array<{ id: ResultTab; label: string; icon: typeof Rows3 }> = [
  { id: "data", label: "数据", icon: Rows3 },
  { id: "logs", label: "日志", icon: FileText },
  { id: "plan", label: "执行计划", icon: ListTree },
];

export function ResultsPane() {
  const activeTab = useWorkbenchStore((state) => state.activeResultTab);
  const setActiveTab = useWorkbenchStore((state) => state.setActiveResultTab);
  const queryState = useWorkbenchStore((state) => state.queryState);
  const columns = useWorkbenchStore((state) => state.columns);
  const rows = useWorkbenchStore((state) => state.rows);
  const logs = useWorkbenchStore((state) => state.logs);
  const error = useWorkbenchStore((state) => state.error);
  const durationMs = useWorkbenchStore((state) => state.durationMs);
  const rowsProcessed = useWorkbenchStore((state) => state.rowsProcessed);
  const connection = useWorkbenchStore((state) => state.connection);

  return (
    <section className="results-pane" aria-label="查询结果">
      <div className="results-tabs">
        <div className="result-tab-list" role="tablist" aria-label="结果视图">
          {tabs.map((tab) => {
            const TabIcon = tab.icon;
            return (
              <button
                type="button"
                role="tab"
                aria-selected={activeTab === tab.id}
                className={`result-tab ${
                  activeTab === tab.id ? "result-tab--active" : ""
                }`}
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
              >
                <TabIcon size={16} aria-hidden="true" />
                {tab.label}
              </button>
            );
          })}
        </div>
        <div className="result-actions">
          {queryState === "success" && (
            <span className="query-summary" aria-live="polite">
              <CheckCircle2 size={15} aria-hidden="true" />
              {rows.length} 行 · {durationMs ?? 0} ms
            </span>
          )}
          <IconAction
            label="筛选结果"
            icon={<Search size={16} aria-hidden="true" />}
          />
          <IconAction
            label="导出结果"
            disabled={rows.length === 0}
            icon={<Download size={16} aria-hidden="true" />}
          />
        </div>
      </div>

      <div className="result-content" role="tabpanel" key={activeTab}>
        {activeTab === "logs" ? (
          <LogView
            queryState={queryState}
            logs={logs}
            error={error}
            durationMs={durationMs}
            rowsProcessed={rowsProcessed}
            preview={connection?.mode === "preview"}
          />
        ) : activeTab === "plan" ? (
          <PlanView
            queryState={queryState}
            columns={columns}
            rows={rows}
          />
        ) : (
          <DataView queryState={queryState} columns={columns} rows={rows} />
        )}
      </div>
    </section>
  );
}

function DataView({
  queryState,
  columns,
  rows,
}: {
  queryState: QueryState;
  columns: DbmsQueryColumn[];
  rows: Array<Array<string | null>>;
}) {
  if (queryState === "running") {
    return (
      <div className="result-empty result-empty--loading" aria-live="polite">
        <span className="loading-orbit" aria-hidden="true" />
        <strong>正在接收结果</strong>
      </div>
    );
  }

  if (queryState !== "success") {
    return (
      <div className="result-empty">
        <Braces size={24} strokeWidth={1.6} aria-hidden="true" />
        <strong>运行查询</strong>
        <span>Ctrl Enter</span>
      </div>
    );
  }

  return (
    <div className="table-scroll">
      <table className="result-table">
        <thead>
          <tr>
            {columns.map((column, index) => (
              <th title={column.dataType} key={`${column.name}:${index}`}>
                {column.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((column, columnIndex) => (
                <td
                  className={columnIndex === 0 ? "cell-id" : undefined}
                  key={`${column.name}:${columnIndex}`}
                >
                  {row[columnIndex] ?? <span className="null-value">NULL</span>}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <div className="inline-empty">查询未返回行</div>}
    </div>
  );
}

function LogView({
  queryState,
  logs,
  error,
  durationMs,
  rowsProcessed,
  preview,
}: {
  queryState: QueryState;
  logs: string[];
  error: DbmsError | null;
  durationMs: number | null;
  rowsProcessed: number;
  preview: boolean;
}) {
  return (
    <div className="log-view" aria-live="polite">
      <div className="log-line">
        <span className="log-level">{preview ? "PREVIEW" : "DBMS"}</span>
        <span>
          {preview
            ? "Fixture 数据，不连接真实数据库。"
            : "事件由当前数据库连接流式返回。"}
        </span>
      </div>
      {logs.map((message, index) => (
        <div className="log-line" key={`${message}:${index}`}>
          <span className="log-level log-level--success">INFO</span>
          <span>{message}</span>
        </div>
      ))}
      {queryState === "success" && (
        <div className="log-line">
          <span className="log-level log-level--success">OK</span>
          <span>
            处理 {rowsProcessed} 行，耗时 {durationMs ?? 0} ms。
          </span>
        </div>
      )}
      {error && (
        <div className="structured-error structured-error--query" role="alert">
          <strong>
            {error.sqlState} · {error.message}
          </strong>
          {error.detail && <span>{error.detail}</span>}
          {error.hint && <span>{error.hint}</span>}
          {error.position !== null && <span>位置 {error.position}</span>}
          <code>{error.queryId}</code>
        </div>
      )}
    </div>
  );
}

function PlanView({
  queryState,
  columns,
  rows,
}: {
  queryState: QueryState;
  columns: DbmsQueryColumn[];
  rows: Array<Array<string | null>>;
}) {
  if (queryState === "running") {
    return (
      <div className="result-empty result-empty--loading">
        <span className="loading-orbit" aria-hidden="true" />
        <strong>正在读取执行计划</strong>
      </div>
    );
  }
  if (queryState !== "success" || rows.length === 0) {
    return (
      <div className="result-empty">
        <ListTree size={24} aria-hidden="true" />
        <strong>运行 Explain</strong>
      </div>
    );
  }
  return (
    <div className="execution-plan" aria-label="执行计划">
      {rows.map((row, index) => (
        <div
          className={`plan-node ${
            index === 0 ? "plan-node--root" : "plan-node--level-1"
          }`}
          key={index}
        >
          {index === 0 && <ListTree size={16} aria-hidden="true" />}
          <span>{row.filter((value) => value !== null).join(" · ")}</span>
          {columns[index] && <span>{columns[index].dataType}</span>}
        </div>
      ))}
    </div>
  );
}
