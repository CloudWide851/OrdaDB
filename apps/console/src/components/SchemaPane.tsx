import {
  Braces,
  ChevronDown,
  ChevronRight,
  Clock3,
  Database,
  Eye,
  FileCode2,
  FolderOpen,
  FolderTree,
  KeyRound,
  Layers3,
  ListOrdered,
  MoreHorizontal,
  Plug,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Server,
  Table2,
  Trash2,
  Workflow,
  Zap,
  type LucideIcon,
} from "lucide-react";
import { useMemo, useState } from "react";
import type { DbmsCatalogObject } from "../lib/dbmsClient";
import { useWorkbenchStore } from "../store/workbench";
import { ConnectionStatusIcon } from "./ConnectionStatusIcon";
import { IconAction } from "./IconAction";

interface CatalogGroup {
  id: string;
  label: string;
  icon: LucideIcon;
  matches: (object: DbmsCatalogObject) => boolean;
}

const catalogGroups: CatalogGroup[] = [
  { id: "tables", label: "表", icon: Table2, matches: (item) => item.kind === "table" },
  {
    id: "collections",
    label: "集合",
    icon: Layers3,
    matches: (item) => item.kind === "collection",
  },
  {
    id: "keyspaces",
    label: "键空间",
    icon: FolderTree,
    matches: (item) => item.kind === "keyspace",
  },
  {
    id: "keys",
    label: "键",
    icon: KeyRound,
    matches: (item) => item.kind === "key",
  },
  {
    id: "columns",
    label: "列",
    icon: ListOrdered,
    matches: (item) => item.kind === "column",
  },
  { id: "views", label: "视图", icon: Eye, matches: (item) => item.kind === "view" },
  {
    id: "materialized-views",
    label: "物化视图",
    icon: Layers3,
    matches: (item) => item.kind === "materializedView",
  },
  {
    id: "sequences",
    label: "序列",
    icon: ListOrdered,
    matches: (item) => item.kind === "sequence",
  },
  {
    id: "indexes",
    label: "索引",
    icon: KeyRound,
    matches: (item) => item.kind === "index",
  },
  {
    id: "constraints",
    label: "约束",
    icon: Braces,
    matches: (item) => item.kind === "constraint",
  },
  {
    id: "routines",
    label: "函数与过程",
    icon: Workflow,
    matches: (item) => item.kind === "routine",
  },
  {
    id: "triggers",
    label: "触发器",
    icon: Zap,
    matches: (item) => item.kind === "trigger",
  },
];

export function SchemaPane() {
  const [filter, setFilter] = useState("");
  const [expandedGroups, setExpandedGroups] = useState(() => new Set<string>());
  const [selectedEntry, setSelectedEntry] = useState<string | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const selectedObject = useWorkbenchStore((state) => state.selectedObject);
  const catalog = useWorkbenchStore((state) => state.catalog);
  const connection = useWorkbenchStore((state) => state.connection);
  const connectionState = useWorkbenchStore((state) => state.connectionState);
  const workspace = useWorkbenchStore((state) => state.workspace);
  const recentFiles = useWorkbenchStore((state) => state.recentFiles);
  const documents = useWorkbenchStore((state) => state.documents);
  const activeDocumentPath = useWorkbenchStore(
    (state) => state.activeDocumentPath,
  );
  const settings = useWorkbenchStore((state) => state.settings);
  const view = useWorkbenchStore((state) => state.sidebarView);
  const setView = useWorkbenchStore((state) => state.setSidebarView);
  const setSelectedObject = useWorkbenchStore(
    (state) => state.setSelectedObject,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const setDataSourceOpen = useWorkbenchStore(
    (state) => state.setDataSourceOpen,
  );
  const refreshCatalog = useWorkbenchStore((state) => state.refreshCatalog);
  const openWorkspace = useWorkbenchStore((state) => state.openWorkspace);
  const openFile = useWorkbenchStore((state) => state.openFile);
  const openRecentFile = useWorkbenchStore((state) => state.openRecentFile);
  const openDocument = useWorkbenchStore((state) => state.openDocument);
  const createDocument = useWorkbenchStore((state) => state.createDocument);
  const renameWorkspaceEntry = useWorkbenchStore(
    (state) => state.renameWorkspaceEntry,
  );
  const trashWorkspaceEntry = useWorkbenchStore(
    (state) => state.trashWorkspaceEntry,
  );

  const visibleGroups = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    return catalogGroups
      .map((group) => {
        const objects = catalog.filter(
          (object) =>
            group.matches(object) &&
            (!normalized ||
              object.name.toLowerCase().includes(normalized) ||
              object.schema.toLowerCase().includes(normalized) ||
              object.namespace?.toLowerCase().includes(normalized)),
        );
        return { ...group, objects };
      })
      .filter(
        (group) =>
          (!settings.appearance.hideEmptyCatalog ||
            group.objects.length > 0) &&
          (!normalized ||
          group.label.toLowerCase().includes(normalized) ||
          group.objects.length > 0),
      );
  }, [catalog, filter, settings.appearance.hideEmptyCatalog]);

  const database =
    catalog.find((object) => object.kind === "database")?.name ??
    connection?.database;
  const schema = catalog.find((object) => object.kind === "schema")?.name;
  const visibleEntries = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    if (!workspace) return [];
    return workspace.entries.filter(
      (entry) =>
        !normalized ||
        entry.name.toLowerCase().includes(normalized) ||
        entry.path.toLowerCase().includes(normalized),
    );
  }, [filter, workspace]);

  const toggleGroup = (groupId: string) => {
    setExpandedGroups((current) => {
      const next = new Set(current);
      if (next.has(groupId)) next.delete(groupId);
      else next.add(groupId);
      return next;
    });
  };

  return (
    <aside className="schema-pane" aria-label="数据库浏览器">
      <div className="pane-heading">
        <h2>{view === "workspace" ? "SQL 项目" : "数据库"}</h2>
        <div className="heading-actions">
          {view === "workspace" ? (
            <>
              <IconAction
                label="打开 SQL 项目"
                icon={<FolderOpen size={16} aria-hidden="true" />}
                onClick={() => void openWorkspace()}
              />
              <IconAction
                label="打开 SQL 文件"
                icon={<FileCode2 size={16} aria-hidden="true" />}
                onClick={() => void openFile()}
              />
              <IconAction
                label="新建 SQL 文件"
                icon={<Plus size={17} aria-hidden="true" />}
                onClick={() => void createDocument()}
              />
            </>
          ) : (
            <>
              <IconAction
                label="连接数据库"
                icon={<Plug size={16} aria-hidden="true" />}
                onClick={() => setDataSourceOpen(true)}
              />
              <IconAction
                label="刷新数据库对象"
                icon={<RefreshCw size={16} aria-hidden="true" />}
                disabled={!connection}
                onClick={() => void refreshCatalog()}
              />
            </>
          )}
        </div>
      </div>

      <div className="explorer-switch" role="tablist" aria-label="侧栏视图">
        <button
          type="button"
          role="tab"
          aria-selected={view === "workspace"}
          className={view === "workspace" ? "explorer-switch--active" : ""}
          onClick={() => setView("workspace")}
        >
          项目
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={view === "database"}
          className={view === "database" ? "explorer-switch--active" : ""}
          onClick={() => setView("database")}
        >
          数据库
        </button>
      </div>

      {((view === "workspace" && workspace) ||
        (view === "database" && connection)) && (
        <label className="schema-search">
          <Search size={16} aria-hidden="true" />
          <span className="sr-only">筛选侧栏项目</span>
          <input
            type="search"
            aria-label="筛选侧栏项目"
            placeholder="筛选"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
        </label>
      )}

      {view === "workspace" ? (
        <nav className="schema-tree workspace-tree" aria-label="SQL 项目文件">
          {!workspace ? (
            <div className="workspace-welcome">
              <button
                type="button"
                className="schema-empty schema-empty--primary"
                onClick={() => void createDocument()}
              >
                <Plus size={16} aria-hidden="true" />
                新建 SQL
              </button>
              <button
                type="button"
                className="schema-empty"
                onClick={() => void openFile()}
              >
                <FileCode2 size={16} aria-hidden="true" />
                打开文件
              </button>
              <button
                type="button"
                className="schema-empty"
                onClick={() => void openWorkspace()}
              >
                <FolderOpen size={16} aria-hidden="true" />
                打开项目
              </button>
              {recentFiles.length > 0 && (
                <div className="recent-files">
                  <strong>
                    <Clock3 size={13} aria-hidden="true" />
                    最近文件
                  </strong>
                  {recentFiles.slice(0, 8).map((entry) => (
                    <button
                      type="button"
                      key={`${entry.locator.kind}:${
                        entry.locator.kind === "workspace"
                          ? `${entry.locator.rootPath}:${entry.locator.path}`
                          : entry.locator.path
                      }`}
                      onClick={() => void openRecentFile(entry)}
                    >
                      <FileCode2 size={13} aria-hidden="true" />
                      <span>{entry.name}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          ) : (
            visibleEntries.map((entry) => {
              const open = documents.some(
                (document) =>
                  document.locator.kind === "workspace" &&
                  document.locator.rootPath === workspace.rootPath &&
                  document.locator.path === entry.path,
              );
              const active = documents.some(
                (document) =>
                  document.path === activeDocumentPath &&
                  document.locator.kind === "workspace" &&
                  document.locator.rootPath === workspace.rootPath &&
                  document.locator.path === entry.path,
              );
              const selected = selectedEntry === entry.path;
              return (
                <div className="workspace-entry" key={entry.path}>
                  {renaming === entry.path ? (
                    <form
                      className="workspace-rename"
                      style={{ paddingInlineStart: `${entry.depth * 12}px` }}
                      onSubmit={(event) => {
                        event.preventDefault();
                        const next = renameValue.trim();
                        if (next) void renameWorkspaceEntry(entry.path, next);
                        setRenaming(null);
                      }}
                    >
                      <input
                        aria-label={`重命名 ${entry.name}`}
                        value={renameValue}
                        onChange={(event) => setRenameValue(event.target.value)}
                        autoFocus
                        onKeyDown={(event) => {
                          if (event.key === "Escape") setRenaming(null);
                        }}
                      />
                    </form>
                  ) : (
                    <button
                      type="button"
                      className={`tree-row tree-row--workspace ${
                        active ? "tree-row--active" : ""
                      } ${selected ? "tree-row--selected" : ""}`}
                      style={{ paddingInlineStart: `${entry.depth * 12}px` }}
                      onClick={() => {
                        setSelectedEntry(entry.path);
                        if (entry.kind === "sqlFile") {
                          void openDocument(entry.path);
                        }
                      }}
                    >
                      {entry.kind === "directory" ? (
                        <FolderTree size={14} aria-hidden="true" />
                      ) : (
                        <FileCode2 size={14} aria-hidden="true" />
                      )}
                      <span>{entry.name}</span>
                      {open && <span className="workspace-open-mark">打开</span>}
                    </button>
                  )}
                </div>
              );
            })
          )}
        </nav>
      ) : !connection ? (
        <div className="schema-tree schema-tree--disconnected">
          <button
            type="button"
            className="schema-empty schema-empty--primary"
            onClick={() => setDataSourceOpen(true)}
          >
            <Plug size={16} aria-hidden="true" />
            连接数据库
          </button>
        </div>
      ) : (
        <nav className="schema-tree" aria-label="数据库对象">
          <button
            type="button"
            className="tree-row tree-row--connection"
            onClick={() => setDataSourceOpen(true)}
          >
            <ChevronDown size={15} aria-hidden="true" />
            <Server size={16} aria-hidden="true" />
            <span>{connection.endpoint}</span>
            <ConnectionStatusIcon
              className="tree-status"
              state={connectionState}
              preview={connection.mode === "preview"}
            />
          </button>
          {database && (
            <div className="tree-row tree-row--database">
              <ChevronDown size={15} aria-hidden="true" />
              <Database size={16} aria-hidden="true" />
              <span>{database}</span>
            </div>
          )}
          {schema && (
            <div className="tree-row tree-row--schema">
              <ChevronDown size={15} aria-hidden="true" />
              <FolderTree size={16} aria-hidden="true" />
              <span>{schema}</span>
            </div>
          )}

          {visibleGroups.map((group) => {
          const expanded = expandedGroups.has(group.id) || filter.length > 0;
          const GroupIcon = group.icon;

          return (
            <div className="object-group" key={group.id}>
              <button
                className="tree-row tree-row--group"
                type="button"
                aria-expanded={expanded}
                onClick={() => toggleGroup(group.id)}
              >
                {expanded ? (
                  <ChevronDown size={14} aria-hidden="true" />
                ) : (
                  <ChevronRight size={14} aria-hidden="true" />
                )}
                <GroupIcon size={15} aria-hidden="true" />
                <span>{group.label}</span>
                <span className="object-count">{group.objects.length}</span>
              </button>
              {expanded &&
                group.objects.map((object) => (
                  <button
                    className={`tree-row tree-row--object ${
                      selectedObject === catalogObjectIdentity(object)
                        ? "tree-row--active"
                        : ""
                    }`}
                    type="button"
                    aria-current={
                      selectedObject === catalogObjectIdentity(object)
                        ? "page"
                        : undefined
                    }
                    key={catalogObjectIdentity(object)}
                    onClick={() => {
                      setSelectedObject(catalogObjectIdentity(object));
                      setNotice(
                        `${(object.namespace ?? object.schema) || "Catalog"}.${object.name} · 已选择`,
                      );
                    }}
                  >
                    <GroupIcon size={14} aria-hidden="true" />
                    <span>{object.name}</span>
                  </button>
                ))}
            </div>
              );
          })}
        </nav>
      )}

      {view === "workspace" && workspace && selectedEntry && (
        <div className="schema-footer">
          <span title={selectedEntry}>{selectedEntry}</span>
          <IconAction
            label="重命名项目条目"
            icon={<Pencil size={14} aria-hidden="true" />}
            onClick={() => {
              setRenaming(selectedEntry);
              setRenameValue(
                workspace.entries.find((entry) => entry.path === selectedEntry)
                  ?.name ?? "",
              );
            }}
          />
          <IconAction
            label="移入回收站"
            icon={<Trash2 size={14} aria-hidden="true" />}
            onClick={() => {
              if (window.confirm(`将 ${selectedEntry} 移入回收站？`)) {
                void trashWorkspaceEntry(selectedEntry);
                setSelectedEntry(null);
              }
            }}
          />
        </div>
      )}
      {view === "database" && connection && (
        <div className="schema-footer">
          <span>{catalog.length} 个目录对象</span>
          <IconAction
            label="数据库浏览器更多操作"
            icon={<MoreHorizontal size={17} aria-hidden="true" />}
          />
        </div>
      )}
    </aside>
  );
}

function catalogObjectIdentity(object: DbmsCatalogObject) {
  return object.id ?? `${object.kind}:${object.schema}:${object.name}`;
}
