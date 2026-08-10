import {
  Bot,
  Command,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
} from "lucide-react";
import type { WorkbenchCommandId } from "../data/commands";
import type { InspectorMode } from "../store/workbench";
import { IconAction } from "./IconAction";

interface CommandToolbarProps {
  schemaVisible: boolean;
  inspectorVisible: boolean;
  inspectorMode: InspectorMode;
  onCommand: (commandId: WorkbenchCommandId) => void;
}

export function CommandToolbar({
  schemaVisible,
  inspectorVisible,
  inspectorMode,
  onCommand,
}: CommandToolbarProps) {
  return (
    <div className="command-toolbar" aria-label="快捷工具">
      <IconAction
        label={schemaVisible ? "隐藏数据库浏览器" : "显示数据库浏览器"}
        icon={
          schemaVisible ? (
            <PanelLeftClose size={17} aria-hidden="true" />
          ) : (
            <PanelLeftOpen size={17} aria-hidden="true" />
          )
        }
        onClick={() => onCommand("toggle-explorer")}
      />
      <IconAction
        label={
          inspectorVisible
            ? inspectorMode === "ai"
              ? "隐藏 AI 助手"
              : "隐藏对象检查器"
            : "显示右侧面板"
        }
        icon={
          inspectorVisible ? (
            <PanelRightClose size={17} aria-hidden="true" />
          ) : (
            <PanelRightOpen size={17} aria-hidden="true" />
          )
        }
        onClick={() => onCommand("toggle-inspector")}
      />
      <IconAction
        label="打开 AI 助手"
        tone={inspectorVisible && inspectorMode === "ai" ? "brand" : "plain"}
        icon={<Bot size={16} aria-hidden="true" />}
        onClick={() => onCommand("ai-workbench")}
      />
      <span className="command-divider" aria-hidden="true" />
      <IconAction
        label="打开命令面板"
        icon={<Command size={17} aria-hidden="true" />}
        onClick={() => onCommand("command-palette")}
      />
    </div>
  );
}
