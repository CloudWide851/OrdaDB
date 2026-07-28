import {
  Command,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
} from "lucide-react";
import type { WorkbenchCommandId } from "../data/commands";
import { IconAction } from "./IconAction";

interface CommandToolbarProps {
  schemaVisible: boolean;
  inspectorVisible: boolean;
  onCommand: (commandId: WorkbenchCommandId) => void;
}

export function CommandToolbar({
  schemaVisible,
  inspectorVisible,
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
        label={inspectorVisible ? "隐藏对象检查器" : "显示对象检查器"}
        icon={
          inspectorVisible ? (
            <PanelRightClose size={17} aria-hidden="true" />
          ) : (
            <PanelRightOpen size={17} aria-hidden="true" />
          )
        }
        onClick={() => onCommand("toggle-inspector")}
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
