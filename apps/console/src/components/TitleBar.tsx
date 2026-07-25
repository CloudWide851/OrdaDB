import { Minus, Square, X } from "lucide-react";
import logoUrl from "../../../../logo.svg?url";
import type { WorkbenchCommandId } from "../data/commands";
import { runWindowAction } from "../lib/tauri";
import { CommandToolbar } from "./CommandToolbar";
import { IconAction } from "./IconAction";
import { MenuBar } from "./MenuBar";

interface TitleBarProps {
  schemaVisible: boolean;
  inspectorVisible: boolean;
  onCommand: (commandId: WorkbenchCommandId) => void;
}

export function TitleBar({
  schemaVisible,
  inspectorVisible,
  onCommand,
}: TitleBarProps) {
  return (
    <header className="titlebar">
      <div className="titlebar-brand" data-tauri-drag-region>
        <img src={logoUrl} alt="" className="brand-logo" />
        <span className="brand-name">OrdaDB</span>
      </div>

      <MenuBar onCommand={onCommand} />
      <CommandToolbar
        schemaVisible={schemaVisible}
        inspectorVisible={inspectorVisible}
        onCommand={onCommand}
      />
      <div
        className="titlebar-drag-region"
        data-tauri-drag-region
        aria-hidden="true"
      />

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
