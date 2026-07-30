export type WorkbenchCommandId =
  | "new-query"
  | "open-file"
  | "open-project"
  | "save-file"
  | "save-as"
  | "save-all"
  | "data-sources"
  | "settings"
  | "undo"
  | "redo"
  | "find"
  | "format-sql"
  | "completion"
  | "toggle-explorer"
  | "toggle-inspector"
  | "database-view"
  | "files-view"
  | "object-inspector"
  | "command-palette"
  | "recent-files"
  | "go-to-file"
  | "focus-navigation"
  | "go-to-object"
  | "sql-history"
  | "run-query"
  | "explain-query"
  | "stop-query"
  | "new-object"
  | "modify-object"
  | "drop-object"
  | "sessions"
  | "locks"
  | "transactions"
  | "roles"
  | "wal-checkpoints"
  | "backup-restore"
  | "import-export"
  | "service-manager"
  | "documentation"
  | "about";

export interface WorkbenchCommand {
  id: WorkbenchCommandId;
  label: string;
  group: string;
  shortcut?: string;
  keywords?: string;
  disabled?: boolean;
}

export interface WorkbenchMenu {
  id: string;
  label: string;
  accessKey: string;
  items: Array<WorkbenchCommandId | "separator">;
}

export interface KeybindingMapV1 {
  formatVersion: 1;
  bindings: Array<{
    commandId: WorkbenchCommandId;
    accelerator: string;
  }>;
}

export const workbenchCommands: WorkbenchCommand[] = [
  { id: "new-query", label: "新建查询", group: "文件", shortcut: "Ctrl+N" },
  { id: "open-file", label: "打开文件…", group: "文件", shortcut: "Ctrl+O" },
  {
    id: "open-project",
    label: "打开项目…",
    group: "文件",
    shortcut: "Ctrl+Shift+O",
  },
  { id: "save-file", label: "保存", group: "文件", shortcut: "Ctrl+S" },
  { id: "save-as", label: "另存为…", group: "文件", shortcut: "Ctrl+Shift+S" },
  { id: "save-all", label: "全部保存", group: "文件" },
  {
    id: "data-sources",
    label: "数据源…",
    group: "文件",
    shortcut: "Ctrl+Alt+Shift+S",
    keywords: "连接 database connection",
  },
  { id: "settings", label: "设置…", group: "文件", shortcut: "Ctrl+," },
  { id: "undo", label: "撤销", group: "编辑", shortcut: "Ctrl+Z" },
  { id: "redo", label: "重做", group: "编辑", shortcut: "Ctrl+Y" },
  { id: "find", label: "查找", group: "编辑", shortcut: "Ctrl+F" },
  {
    id: "format-sql",
    label: "格式化 SQL",
    group: "编辑",
    shortcut: "Ctrl+Alt+L",
  },
  { id: "completion", label: "代码补全", group: "编辑", shortcut: "Ctrl+Space" },
  {
    id: "toggle-explorer",
    label: "数据库浏览器",
    group: "视图",
  },
  {
    id: "toggle-inspector",
    label: "对象检查器",
    group: "视图",
  },
  { id: "database-view", label: "数据库视图", group: "视图", shortcut: "Alt+1" },
  { id: "files-view", label: "文件视图", group: "视图", shortcut: "Alt+2" },
  {
    id: "object-inspector",
    label: "对象检查器",
    group: "视图",
    shortcut: "Alt+3",
  },
  {
    id: "command-palette",
    label: "命令面板",
    group: "视图",
    shortcut: "Ctrl+Shift+P",
  },
  { id: "go-to-object", label: "跳转到对象…", group: "导航", shortcut: "Ctrl+B" },
  {
    id: "recent-files",
    label: "最近文件",
    group: "导航",
    shortcut: "Ctrl+E",
  },
  {
    id: "go-to-file",
    label: "转到文件…",
    group: "导航",
    shortcut: "Ctrl+Shift+N",
  },
  {
    id: "focus-navigation",
    label: "聚焦导航栏",
    group: "导航",
    shortcut: "Alt+Home",
  },
  {
    id: "sql-history",
    label: "SQL 历史",
    group: "导航",
    shortcut: "Ctrl+Alt+H",
  },
  {
    id: "run-query",
    label: "运行语句",
    group: "运行",
    shortcut: "Ctrl+Enter",
  },
  {
    id: "explain-query",
    label: "解释执行计划",
    group: "运行",
    shortcut: "Ctrl+Alt+E",
  },
  { id: "stop-query", label: "停止查询", group: "运行", shortcut: "Ctrl+F2" },
  { id: "new-object", label: "新建对象…", group: "数据库" },
  { id: "modify-object", label: "修改对象…", group: "数据库" },
  { id: "drop-object", label: "删除对象…", group: "数据库" },
  { id: "sessions", label: "会话", group: "工具" },
  { id: "locks", label: "锁监控", group: "工具" },
  { id: "transactions", label: "事务监控", group: "工具" },
  { id: "roles", label: "用户与角色", group: "工具" },
  { id: "wal-checkpoints", label: "WAL 与检查点", group: "工具" },
  { id: "backup-restore", label: "备份与恢复…", group: "工具" },
  { id: "import-export", label: "导入与导出…", group: "工具" },
  { id: "service-manager", label: "服务管理", group: "窗口" },
  { id: "documentation", label: "文档", group: "帮助", shortcut: "F1" },
  { id: "about", label: "关于 OrdaDB", group: "帮助" },
];

export const defaultKeybindings: KeybindingMapV1 = {
  formatVersion: 1,
  bindings: workbenchCommands
    .filter(
      (
        command,
      ): command is WorkbenchCommand & {
        shortcut: string;
      } => Boolean(command.shortcut),
    )
    .map((command) => ({
      commandId: command.id,
      accelerator: command.shortcut,
    })),
};

export function commandForKeyboardEvent(
  event: KeyboardEvent,
  keybindings: KeybindingMapV1 = defaultKeybindings,
) {
  return keybindings.bindings.find((binding) =>
    matchesAccelerator(event, binding.accelerator),
  )?.commandId;
}

function matchesAccelerator(event: KeyboardEvent, accelerator: string) {
  const parts = accelerator.toLowerCase().split("+");
  const key = parts.at(-1);
  const expectsCtrl = parts.includes("ctrl");
  const expectsAlt = parts.includes("alt");
  const expectsShift = parts.includes("shift");
  const expectsMeta = parts.includes("meta");
  return (
    (event.key.toLowerCase() === key ||
      (key === "enter" && event.key === "Enter") ||
      (key === "home" && event.key === "Home") ||
      (key === "," && event.key === ",")) &&
    event.ctrlKey === expectsCtrl &&
    event.altKey === expectsAlt &&
    event.shiftKey === expectsShift &&
    event.metaKey === expectsMeta
  );
}

export const commandById = new Map(
  workbenchCommands.map((command) => [command.id, command]),
);

export const workbenchMenus: WorkbenchMenu[] = [
  {
    id: "file",
    label: "文件",
    accessKey: "F",
    items: [
      "new-query",
      "open-file",
      "open-project",
      "save-file",
      "save-as",
      "save-all",
      "separator",
      "data-sources",
      "settings",
    ],
  },
  {
    id: "edit",
    label: "编辑",
    accessKey: "E",
    items: [
      "undo",
      "redo",
      "separator",
      "find",
      "format-sql",
      "completion",
    ],
  },
  {
    id: "view",
    label: "视图",
    accessKey: "V",
    items: [
      "database-view",
      "files-view",
      "object-inspector",
      "separator",
      "toggle-explorer",
      "toggle-inspector",
      "command-palette",
    ],
  },
  {
    id: "navigate",
    label: "导航",
    accessKey: "N",
    items: [
      "recent-files",
      "go-to-file",
      "focus-navigation",
      "separator",
      "go-to-object",
      "sql-history",
    ],
  },
  {
    id: "run",
    label: "运行",
    accessKey: "R",
    items: ["run-query", "explain-query", "stop-query"],
  },
  {
    id: "database",
    label: "数据库",
    accessKey: "D",
    items: ["data-sources", "separator", "new-object", "modify-object", "drop-object"],
  },
  {
    id: "tools",
    label: "工具",
    accessKey: "T",
    items: [
      "sessions",
      "locks",
      "transactions",
      "roles",
      "separator",
      "wal-checkpoints",
      "backup-restore",
      "import-export",
    ],
  },
  {
    id: "window",
    label: "窗口",
    accessKey: "W",
    items: ["service-manager", "separator", "toggle-explorer", "toggle-inspector"],
  },
  {
    id: "help",
    label: "帮助",
    accessKey: "H",
    items: ["documentation", "about"],
  },
];
