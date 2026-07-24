import {
  Braces,
  CheckCircle2,
  Download,
  FileText,
  Rows3,
  Search,
} from "lucide-react";
import { useWorkbenchStore } from "../store/workbench";
import type { ResultTab } from "../types";
import { IconAction } from "./IconAction";

const tabs: Array<{ id: ResultTab; label: string; icon: typeof Rows3 }> = [
  { id: "data", label: "结果", icon: Rows3 },
  { id: "logs", label: "日志", icon: FileText },
];

export function ResultsPane() {
  const activeTab = useWorkbenchStore((state) => state.activeResultTab);
  const setActiveTab = useWorkbenchStore((state) => state.setActiveResultTab);
  const queryState = useWorkbenchStore((state) => state.queryState);
  const rows = useWorkbenchStore((state) => state.rows);
  const durationMs = useWorkbenchStore((state) => state.durationMs);
  const errorMessage = useWorkbenchStore((state) => state.errorMessage);

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
              {rows.length} 行 · {durationMs} ms
            </span>
          )}
          <IconAction
            label="筛选结果"
            icon={<Search size={16} aria-hidden="true" />}
          />
          <IconAction
            label="导出结果"
            icon={<Download size={16} aria-hidden="true" />}
          />
        </div>
      </div>

      <div className="result-content" role="tabpanel">
        {activeTab === "logs" ? (
          <LogView
            queryState={queryState}
            errorMessage={errorMessage}
            durationMs={durationMs}
          />
        ) : (
          <DataView queryState={queryState} rows={rows} />
        )}
      </div>
    </section>
  );
}

function DataView({
  queryState,
  rows,
}: {
  queryState: ReturnType<typeof useWorkbenchStore.getState>["queryState"];
  rows: ReturnType<typeof useWorkbenchStore.getState>["rows"];
}) {
  if (queryState === "running") {
    return (
      <div className="result-empty result-empty--loading" aria-live="polite">
        <span className="loading-orbit" aria-hidden="true" />
        <strong>正在生成预览结果</strong>
      </div>
    );
  }

  if (queryState !== "success") {
    return (
      <div className="result-empty">
        <Braces size={24} strokeWidth={1.6} aria-hidden="true" />
        <strong>运行查询以查看预览结果</strong>
        <span>使用运行按钮或 Ctrl Enter</span>
      </div>
    );
  }

  return (
    <div className="table-scroll">
      <table className="result-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>TITLE</th>
            <th>CATEGORY</th>
            <th>SCORE</th>
            <th>UPDATED_AT</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td className="cell-id">{row.id}</td>
              <td className="cell-title">{row.title}</td>
              <td>
                <span className="category-chip">{row.category}</span>
              </td>
              <td className="cell-score">{row.score.toFixed(3)}</td>
              <td>{row.updatedAt}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LogView({
  queryState,
  errorMessage,
  durationMs,
}: {
  queryState: ReturnType<typeof useWorkbenchStore.getState>["queryState"];
  errorMessage: string | null;
  durationMs: number | null;
}) {
  return (
    <div className="log-view" aria-live="polite">
      <div className="log-line">
        <span className="log-time">15:42:08</span>
        <span className="log-level">PREVIEW</span>
        <span>查询将在本地示例数据上执行，不会连接真实数据库。</span>
      </div>
      {queryState === "success" && (
        <div className="log-line">
          <span className="log-time">15:42:09</span>
          <span className="log-level log-level--success">OK</span>
          <span>返回 5 行，耗时 {durationMs} ms。</span>
        </div>
      )}
      {queryState === "error" && (
        <div className="log-line log-line--error">
          <span className="log-time">15:42:09</span>
          <span className="log-level log-level--error">ERROR</span>
          <span>{errorMessage}</span>
        </div>
      )}
    </div>
  );
}
