import { Circle, GitBranch, ShieldCheck } from "lucide-react";
import { getSqlDialect } from "../data/dialects";
import { useWorkbenchStore } from "../store/workbench";
import type { AppStatus } from "../types";

interface StatusBarProps {
  status?: AppStatus;
  loading: boolean;
}

export function StatusBar({ status, loading }: StatusBarProps) {
  const notice = useWorkbenchStore((state) => state.notice);
  const dialect = useWorkbenchStore((state) => state.dialect);
  const connection = useWorkbenchStore((state) => state.connection);
  const connectionState = useWorkbenchStore((state) => state.connectionState);
  const transactionActive = useWorkbenchStore(
    (state) => state.transactionActive,
  );
  const dialectLabel = getSqlDialect(dialect).label;
  const modeLabel = loading
    ? "状态检查中"
    : status?.mode === "desktop"
      ? "本地桌面"
      : "界面预览";

  return (
    <footer className="status-bar" aria-label="工作台状态">
      <div className="status-primary" aria-live="polite">
        <Circle size={8} fill="currentColor" strokeWidth={0} aria-hidden="true" />
        <span>{connection?.database ?? "未连接"}</span>
        <span className="status-separator" aria-hidden="true" />
        <span>{notice}</span>
      </div>
      <div className="status-details">
        <span>
          <ShieldCheck size={14} aria-hidden="true" />
          {connection?.mode === "preview" ? "PREVIEW" : connectionState}
        </span>
        <span>
          <GitBranch size={14} aria-hidden="true" />
          {transactionActive ? "事务进行中" : "自动提交"}
        </span>
        <span>UTF-8</span>
        <span>SQL · {dialectLabel}</span>
        <span>{modeLabel}</span>
        <span>v{status?.version ?? "0.1.0"}</span>
      </div>
    </footer>
  );
}
