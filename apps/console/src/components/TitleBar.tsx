import { Minus, Square, X } from "lucide-react";
import logoUrl from "../../../../logo.svg?url";
import { runWindowAction } from "../lib/tauri";
import { IconAction } from "./IconAction";

export function TitleBar() {
  return (
    <header className="titlebar">
      <div className="titlebar-identity" data-tauri-drag-region>
        <img src={logoUrl} alt="" className="brand-logo" />
        <span className="brand-name">OrdaDB</span>
        <span className="brand-divider" aria-hidden="true" />
        <span className="brand-context">OrdaDB Local / default</span>
      </div>

      <span className="titlebar-document" data-tauri-drag-region>
        query_01.sql
      </span>

      <div className="window-controls">
        <IconAction
          label="最小化窗口"
          className="window-control window-control--minimize"
          icon={<Minus size={16} strokeWidth={1.8} aria-hidden="true" />}
          onClick={() => void runWindowAction("minimize")}
        />
        <IconAction
          label="最大化或还原窗口"
          className="window-control window-control--maximize"
          icon={<Square size={13} strokeWidth={1.8} aria-hidden="true" />}
          onClick={() => void runWindowAction("toggleMaximize")}
        />
        <IconAction
          label="关闭窗口"
          tone="danger"
          className="window-control window-control--close"
          icon={<X size={17} strokeWidth={1.8} aria-hidden="true" />}
          onClick={() => void runWindowAction("close")}
        />
      </div>
    </header>
  );
}
