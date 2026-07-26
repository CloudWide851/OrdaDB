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
        <span>OrdaDB Local</span>
        <span className="status-separator" aria-hidden="true" />
        <span>{notice}</span>
      </div>
      <div className="status-details">
        <span>
          <ShieldCheck size={14} aria-hidden="true" />
          事务预览
        </span>
        <span>
          <GitBranch size={14} aria-hidden="true" />
          自动提交 · 预览
        </span>
        <span>UTF-8</span>
        <span>SQL · {dialectLabel}</span>
        <span>Ln 5, Col 12</span>
        <span>{modeLabel}</span>
        <span>v{status?.version ?? "0.1.0"}</span>
      </div>
    </footer>
  );
}
