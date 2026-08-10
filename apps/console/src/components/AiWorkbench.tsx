import {
  Bot,
  Check,
  CircleAlert,
  Clock3,
  LoaderCircle,
  Send,
  ShieldAlert,
  Square,
  X,
} from "lucide-react";
import {
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

const toolLabels: Record<string, string> = {
  catalog: "Catalog",
  describe_object: "对象描述",
  explain: "执行计划",
  query: "只读查询",
  validate_sql: "SQL 校验",
  repair_sql: "SQL 修复",
  execute_sql: "执行 SQL",
  configure_session: "会话配置",
  backup: "备份",
  restore: "恢复",
  import: "导入",
  export: "导出",
  checkpoint: "检查点",
  service: "服务操作",
};

export function AiWorkbench() {
  const runtimeMode = useWorkbenchStore((state) => state.aiRuntimeMode);
  const connection = useWorkbenchStore((state) => state.connection);
  const settings = useWorkbenchStore((state) => state.settings.ai);
  const messages = useWorkbenchStore((state) => state.aiMessages);
  const tools = useWorkbenchStore((state) => state.aiTools);
  const audit = useWorkbenchStore((state) => state.aiAudit);
  const disclosures = useWorkbenchStore((state) => state.aiDisclosures);
  const approval = useWorkbenchStore((state) => state.aiApproval);
  const usage = useWorkbenchStore((state) => state.aiUsage);
  const runId = useWorkbenchStore((state) => state.aiRunId);
  const runStatus = useWorkbenchStore((state) => state.aiRunStatus);
  const error = useWorkbenchStore((state) => state.aiError);
  const startRun = useWorkbenchStore((state) => state.startAiRun);
  const cancelRun = useWorkbenchStore((state) => state.cancelAiRun);
  const decideApproval = useWorkbenchStore(
    (state) => state.decideAiApproval,
  );
  const setInspectorVisible = useWorkbenchStore(
    (state) => state.setInspectorVisible,
  );
  const [draft, setDraft] = useState("");
  const [includeSamples, setIncludeSamples] = useState(false);
  const messageEndRef = useRef<HTMLDivElement>(null);
  const denyRef = useRef<HTMLButtonElement>(null);
  const running = runId !== null;
  const desktopDisconnected = runtimeMode === "desktop" && !connection;

  useEffect(() => {
    const messageEnd = messageEndRef.current;
    if (typeof messageEnd?.scrollIntoView === "function") {
      messageEnd.scrollIntoView({ block: "nearest" });
    }
  }, [messages, tools, approval]);

  useEffect(() => {
    if (approval) denyRef.current?.focus();
  }, [approval]);

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const text = draft.trim();
    if (!text || running || desktopDisconnected) return;
    setDraft("");
    void startRun(text, includeSamples);
  };

  const historicalAudit = tools.length === 0 ? audit.slice(-4) : [];

  return (
    <aside className="ai-workbench" aria-label="AI 助手">
      <header className="ai-workbench__heading">
        <div>
          <Bot size={15} aria-hidden="true" />
          <h2>AI 助手</h2>
          <span className="ai-mode-badge">
            {runtimeMode === "preview" ? "Preview · 不执行" : "Desktop"}
          </span>
        </div>
        <IconAction
          label="关闭 AI 助手"
          icon={<X size={15} aria-hidden="true" />}
          onClick={() => setInspectorVisible(false)}
        />
      </header>

      <div className="ai-workbench__body">
        {messages.length === 0 ? (
          <div className="ai-empty">
            <Bot size={19} aria-hidden="true" />
            <strong>直接描述数据库问题</strong>
            <span>解释 Schema、修复 SQL，或生成可审计的查询步骤。</span>
          </div>
        ) : (
          <div className="ai-messages" role="log" aria-live="polite">
            {messages.map((message) => (
              <article
                className={`ai-message ai-message--${message.role}`}
                key={message.id}
              >
                <span>{message.role === "user" ? "你" : "OrdaDB AI"}</span>
                <p>{message.text}</p>
              </article>
            ))}
            <div ref={messageEndRef} />
          </div>
        )}

        {disclosures.length > 0 && (
          <section className="ai-disclosures" aria-label="发送给模型的上下文">
            <header>
              <ShieldAlert size={13} aria-hidden="true" />
              <strong>上下文披露</strong>
            </header>
            {disclosures.slice(-3).map((disclosure, index) => (
              <div key={`${disclosure.categories.join(":")}:${index}`}>
                <span>{disclosure.categories.join(" · ")}</span>
                <small>
                  {disclosure.valuesIncluded
                    ? `${disclosure.itemCount} 项 · ${disclosure.redactionSummary}`
                    : disclosure.redactionSummary}
                </small>
              </div>
            ))}
          </section>
        )}

        {(tools.length > 0 || historicalAudit.length > 0) && (
          <section className="ai-tool-audit" aria-label="AI 工具审计">
            <header>
              <strong>工具审计</strong>
              <span>{tools.length || historicalAudit.length}/16</span>
            </header>
            {tools.map((tool) => (
              <div className="ai-tool-row" key={tool.callId}>
                <ToolStatusIcon status={tool.status} />
                <div>
                  <strong>{toolLabels[tool.toolName] ?? tool.toolName}</strong>
                  <span>{tool.summary ?? toolStatusLabel(tool.status)}</span>
                </div>
                {tool.truncated && <small>已截断</small>}
              </div>
            ))}
            {historicalAudit.map((entry) => (
              <div
                className="ai-tool-row ai-tool-row--historical"
                key={`${entry.runId}:${entry.toolCallId}:${entry.createdAtMs}`}
              >
                <Check size={13} aria-hidden="true" />
                <div>
                  <strong>{toolLabels[entry.toolName] ?? entry.toolName}</strong>
                  <span>{entry.summary}</span>
                </div>
                <small>{entry.status}</small>
              </div>
            ))}
          </section>
        )}

        {approval && (
          <section className="ai-approval" role="alert" aria-labelledby="ai-approval-title">
            <header>
              <ShieldAlert size={15} aria-hidden="true" />
              <strong id="ai-approval-title">需要确认</strong>
              <span>
                <Clock3 size={11} aria-hidden="true" />
                {Math.ceil(approval.expiresInMs / 1_000)} 秒
              </span>
            </header>
            <p>{approval.impactSummary}</p>
            <code>{approval.preview}</code>
            <div>
              <button
                ref={denyRef}
                className="secondary-action"
                type="button"
                onClick={() => void decideApproval(false)}
              >
                拒绝
              </button>
              <button
                className="danger-action"
                type="button"
                onClick={() => void decideApproval(true)}
              >
                确认执行
              </button>
            </div>
          </section>
        )}

        {error && (
          <section className="ai-error" role="alert">
            <CircleAlert size={14} aria-hidden="true" />
            <div>
              <strong>{error.sqlState}</strong>
              <span>{error.message}</span>
              {error.hint && <small>{error.hint}</small>}
            </div>
          </section>
        )}
      </div>

      <form className="ai-composer" onSubmit={submit}>
        {desktopDisconnected && (
          <div className="ai-connection-required" role="status">
            先连接数据源，再启动桌面 AI 任务。
          </div>
        )}
        <textarea
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && event.ctrlKey) {
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }
          }}
          placeholder="询问 Schema、SQL 或错误…"
          aria-label="询问 OrdaDB AI"
          disabled={running || desktopDisconnected}
          rows={3}
        />
        <div className="ai-composer__actions">
          <label>
            <input
              type="checkbox"
              checked={includeSamples}
              disabled={running || settings.dataSharing === "schemaOnly"}
              onChange={(event) => setIncludeSamples(event.target.checked)}
            />
            本轮脱敏样例
          </label>
          <span aria-live="polite">{runStatusLabel(runStatus)}</span>
          {running ? (
            <button
              className="secondary-action"
              type="button"
              onClick={() => void cancelRun()}
            >
              <Square size={11} aria-hidden="true" />
              取消
            </button>
          ) : (
            <button
              className="primary-action"
              type="submit"
              disabled={!draft.trim() || desktopDisconnected}
            >
              <Send size={12} aria-hidden="true" />
              发送
            </button>
          )}
        </div>
        {usage && (
          <small className="ai-usage">
            输入 {usage.inputTokens} · 输出 {usage.outputTokens} · 推理 {usage.reasoningTokens}
          </small>
        )}
      </form>
    </aside>
  );
}

function ToolStatusIcon({
  status,
}: {
  status: "proposed" | "waitingApproval" | "running" | "completed";
}) {
  let icon: ReactNode;
  if (status === "completed") {
    icon = <Check size={13} aria-hidden="true" />;
  } else if (status === "waitingApproval") {
    icon = <ShieldAlert size={13} aria-hidden="true" />;
  } else {
    icon = <LoaderCircle size={13} aria-hidden="true" />;
  }
  return <span className={`ai-tool-status ai-tool-status--${status}`}>{icon}</span>;
}

function toolStatusLabel(status: string) {
  switch (status) {
    case "proposed":
      return "已提议";
    case "waitingApproval":
      return "等待确认";
    case "running":
      return "正在执行";
    case "completed":
      return "已完成";
    default:
      return status;
  }
}

function runStatusLabel(status: string) {
  switch (status) {
    case "running":
      return "生成中";
    case "waitingApproval":
      return "等待确认";
    case "completed":
      return "已完成";
    case "cancelled":
      return "已取消";
    case "error":
      return "失败";
    default:
      return "就绪";
  }
}
