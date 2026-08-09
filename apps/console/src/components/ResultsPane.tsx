import {
  Braces,
  CheckCircle2,
  Download,
  FileText,
  ListTree,
  Rows3,
  Search,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type {
  DbmsError,
  DbmsQueryColumn,
} from "../lib/dbmsClient";
import {
  resultRowAt,
  resultRows,
  type ResultPage,
} from "../lib/resultBuffer";
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
  const resultBuffer = useWorkbenchStore((state) => state.resultBuffer);
  const documentResults = useWorkbenchStore((state) => state.documentResults);
  const keyValueResults = useWorkbenchStore((state) => state.keyValueResults);
  const droppedStructuredItems = useWorkbenchStore(
    (state) => state.droppedStructuredItems,
  );
  const logs = useWorkbenchStore((state) => state.logs);
  const error = useWorkbenchStore((state) => state.error);
  const durationMs = useWorkbenchStore((state) => state.durationMs);
  const rowsProcessed = useWorkbenchStore((state) => state.rowsProcessed);
  const nullDisplay = useWorkbenchStore(
    (state) => state.settings.results.nullDisplay,
  );
  const residentItems =
    resultBuffer.rowCount + documentResults.length + keyValueResults.length;
  const totalItems = Math.max(
    resultBuffer.totalRows +
      documentResults.length +
      keyValueResults.length +
      droppedStructuredItems,
    rowsProcessed,
  );
  const planRows = useMemo(
    () => (activeTab === "plan" ? resultRows(resultBuffer.pages) : []),
    [activeTab, resultBuffer.pages],
  );

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
              {residentItems === totalItems
                ? `${totalItems} 项`
                : `显示 ${residentItems} / ${totalItems} 项`}{" "}
              · {durationMs ?? 0} ms
            </span>
          )}
          <IconAction
            label="筛选结果"
            icon={<Search size={16} aria-hidden="true" />}
          />
          <IconAction
            label="导出结果"
            disabled={residentItems === 0}
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
          />
        ) : activeTab === "plan" ? (
          <PlanView
            queryState={queryState}
            columns={columns}
            rows={planRows}
          />
        ) : (
          <DataView
            queryState={queryState}
            columns={columns}
            pages={resultBuffer.pages}
            rowCount={resultBuffer.rowCount}
            droppedRows={resultBuffer.droppedRows}
            documents={documentResults}
            keyValues={keyValueResults}
            droppedStructuredItems={droppedStructuredItems}
            nullDisplay={nullDisplay}
          />
        )}
      </div>
    </section>
  );
}

function DataView({
  queryState,
  columns,
  pages,
  rowCount,
  droppedRows,
  documents,
  keyValues,
  droppedStructuredItems,
  nullDisplay,
}: {
  queryState: QueryState;
  columns: DbmsQueryColumn[];
  pages: ResultPage[];
  rowCount: number;
  droppedRows: number;
  documents: unknown[];
  keyValues: Array<{ key: unknown; value: unknown }>;
  droppedStructuredItems: number;
  nullDisplay: string;
}) {
  if (queryState === "running" && rowCount === 0) {
    return (
      <div className="result-empty result-empty--loading" aria-live="polite">
        <span className="loading-orbit" aria-hidden="true" />
        <strong>正在接收结果</strong>
      </div>
    );
  }

  if (queryState !== "success" && queryState !== "running") {
    return (
      <div className="result-empty">
        <Braces size={24} strokeWidth={1.6} aria-hidden="true" />
        <strong>运行命令</strong>
        <span>Ctrl Enter</span>
      </div>
    );
  }

  if (documents.length > 0) {
    return (
      <StructuredResultList
        values={documents}
        droppedItems={droppedStructuredItems}
      />
    );
  }

  if (keyValues.length > 0) {
    return (
      <KeyValueResultList
        entries={keyValues}
        droppedItems={droppedStructuredItems}
      />
    );
  }

  return (
    <VirtualResultTable
      columns={columns}
      pages={pages}
      rowCount={rowCount}
      droppedRows={droppedRows}
      nullDisplay={nullDisplay}
    />
  );
}

function StructuredResultList({
  values,
  droppedItems,
}: {
  values: unknown[];
  droppedItems: number;
}) {
  return (
    <div className="structured-result-list" aria-label="文档结果">
      {values.map((value, index) => (
        <article className="structured-result-card" key={index}>
          <span>#{index + 1}</span>
          <pre>{formatStructuredValue(value)}</pre>
        </article>
      ))}
      {droppedItems > 0 && (
        <div className="result-buffer-note" role="status">
          已保留前 {values.length} 项，另有 {droppedItems} 项未驻留。
        </div>
      )}
    </div>
  );
}

function KeyValueResultList({
  entries,
  droppedItems,
}: {
  entries: Array<{ key: unknown; value: unknown }>;
  droppedItems: number;
}) {
  return (
    <div className="table-scroll" aria-label="键值结果">
      <table className="result-table key-value-result-table">
        <thead>
          <tr>
            <th>Key</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((entry, index) => (
            <tr key={index}>
              <td className="cell-id">
                <code>{formatCompactValue(entry.key)}</code>
              </td>
              <td>
                <pre>{formatStructuredValue(entry.value)}</pre>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {droppedItems > 0 && (
        <div className="result-buffer-note" role="status">
          已保留前 {entries.length} 项，另有 {droppedItems} 项未驻留。
        </div>
      )}
    </div>
  );
}

function formatStructuredValue(value: unknown) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) ?? "null";
  } catch {
    return "[无法序列化的值]";
  }
}

function formatCompactValue(value: unknown) {
  return formatStructuredValue(value).replace(/\s+/gu, " ");
}

const RESULT_ROW_HEIGHT = 32;
const RESULT_OVERSCAN_ROWS = 8;

function VirtualResultTable({
  columns,
  pages,
  rowCount,
  droppedRows,
  nullDisplay,
}: {
  columns: DbmsQueryColumn[];
  pages: ResultPage[];
  rowCount: number;
  droppedRows: number;
  nullDisplay: string;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(320);

  useEffect(() => {
    const element = viewport.current;
    if (!element) {
      return;
    }
    const updateHeight = () => setViewportHeight(element.clientHeight || 320);
    updateHeight();
    if (typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver(updateHeight);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const start = Math.max(
    0,
    Math.floor(scrollTop / RESULT_ROW_HEIGHT) - RESULT_OVERSCAN_ROWS,
  );
  const visibleCount =
    Math.ceil(viewportHeight / RESULT_ROW_HEIGHT) + RESULT_OVERSCAN_ROWS * 2;
  const end = Math.min(rowCount, start + visibleCount);
  const visibleRows = useMemo(
    () =>
      Array.from({ length: end - start }, (_, offset) => ({
        index: start + offset,
        row: resultRowAt(pages, start + offset),
      })),
    [end, pages, start],
  );
  const topHeight = start * RESULT_ROW_HEIGHT;
  const bottomHeight = Math.max(0, (rowCount - end) * RESULT_ROW_HEIGHT);
  const columnSpan = Math.max(1, columns.length);

  return (
    <div
      className="table-scroll"
      ref={viewport}
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
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
          {topHeight > 0 && (
            <tr className="result-spacer" aria-hidden="true">
              <td colSpan={columnSpan} style={{ height: topHeight }} />
            </tr>
          )}
          {visibleRows.map(({ row, index: rowIndex }) => (
            <tr key={rowIndex} style={{ height: RESULT_ROW_HEIGHT }}>
              {columns.map((column, columnIndex) => (
                <td
                  className={columnIndex === 0 ? "cell-id" : undefined}
                  key={`${column.name}:${columnIndex}`}
                >
                  {row?.[columnIndex] ?? (
                    <span className="null-value">{nullDisplay}</span>
                  )}
                </td>
              ))}
            </tr>
          ))}
          {bottomHeight > 0 && (
            <tr className="result-spacer" aria-hidden="true">
              <td colSpan={columnSpan} style={{ height: bottomHeight }} />
            </tr>
          )}
        </tbody>
      </table>
      {rowCount === 0 && <div className="inline-empty">查询未返回行</div>}
      {droppedRows > 0 && (
        <div className="result-buffer-note" role="status">
          已保留前 {rowCount} 行，另有 {droppedRows} 行未驻留。
        </div>
      )}
    </div>
  );
}

function LogView({
  queryState,
  logs,
  error,
  durationMs,
  rowsProcessed,
}: {
  queryState: QueryState;
  logs: string[];
  error: DbmsError | null;
  durationMs: number | null;
  rowsProcessed: number;
}) {
  return (
    <div className="log-view" aria-live="polite">
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
