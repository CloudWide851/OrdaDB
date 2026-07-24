import { Circle, Minus, PanelTop, X } from "lucide-react";
import logoUrl from "../../../../logo.svg?url";
import { runWindowAction } from "../lib/tauri";
import type { AppStatus } from "../types";
import { IconAction } from "./IconAction";

interface TitleBarProps {
  status?: AppStatus;
  loading: boolean;
}

export function TitleBar({ status, loading }: TitleBarProps) {
  const modeLabel = loading
    ? "连接中"
    : status?.mode === "desktop"
      ? "本地引擎"
      : "界面预览";

  return (
    <header className="titlebar" data-tauri-drag-region>
      <div className="window-controls" data-tauri-drag-region="false">
        <IconAction
          label="关闭窗口"
          tone="danger"
          className="window-control window-control--close"
          icon={<X size={12} strokeWidth={2.4} aria-hidden="true" />}
          onClick={() => void runWindowAction("close")}
        />
        <IconAction
          label="最小化窗口"
          className="window-control window-control--minimize"
          icon={<Minus size={12} strokeWidth={2.4} aria-hidden="true" />}
          onClick={() => void runWindowAction("minimize")}
        />
        <IconAction
          label="最大化窗口"
          className="window-control window-control--maximize"
          icon={<PanelTop size={11} strokeWidth={2.2} aria-hidden="true" />}
          onClick={() => void runWindowAction("toggleMaximize")}
        />
      </div>

      <div className="brand-lockup" data-tauri-drag-region>
        <img src={logoUrl} alt="" className="brand-logo" />
        <span className="brand-name">OrdaDB</span>
        <span className="brand-divider" aria-hidden="true" />
        <span className="brand-context">SQL 工作台</span>
      </div>

      <div className="engine-status" aria-live="polite" data-tauri-drag-region>
        <Circle
          size={9}
          fill="currentColor"
          strokeWidth={0}
          aria-hidden="true"
        />
        <span>{modeLabel}</span>
        <span className="engine-version">{status?.version ?? "0.1.0"}</span>
      </div>
    </header>
  );
}
