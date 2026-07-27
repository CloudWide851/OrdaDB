import {
  Braces,
  ChevronDown,
  ChevronRight,
  Database,
  Eye,
  FolderTree,
  KeyRound,
  Layers3,
  ListOrdered,
  MoreHorizontal,
  Plug,
  PlugZap,
  Plus,
  RefreshCw,
  Search,
  Server,
  Table2,
  Workflow,
  Zap,
  type LucideIcon,
} from "lucide-react";
import { useMemo, useState } from "react";
import type { DbmsCatalogObject } from "../lib/dbmsClient";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

interface CatalogGroup {
  id: string;
  label: string;
  icon: LucideIcon;
  matches: (object: DbmsCatalogObject) => boolean;
}

const catalogGroups: CatalogGroup[] = [
  { id: "tables", label: "表", icon: Table2, matches: (item) => item.kind === "table" },
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
  const [expandedGroups, setExpandedGroups] = useState(
    () => new Set(["tables", "views", "indexes"]),
  );
  const selectedObject = useWorkbenchStore((state) => state.selectedObject);
  const catalog = useWorkbenchStore((state) => state.catalog);
  const connection = useWorkbenchStore((state) => state.connection);
  const connectionState = useWorkbenchStore((state) => state.connectionState);
  const setSelectedObject = useWorkbenchStore(
    (state) => state.setSelectedObject,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const setPluginManagerOpen = useWorkbenchStore(
    (state) => state.setPluginManagerOpen,
  );
  const setDataSourceOpen = useWorkbenchStore(
    (state) => state.setDataSourceOpen,
  );
  const refreshCatalog = useWorkbenchStore((state) => state.refreshCatalog);

  const visibleGroups = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    return catalogGroups
      .map((group) => {
        const objects = catalog.filter(
          (object) =>
            group.matches(object) &&
            (!normalized ||
              object.name.toLowerCase().includes(normalized) ||
              object.schema.toLowerCase().includes(normalized)),
        );
        return { ...group, objects };
      })
      .filter(
        (group) =>
          !normalized ||
          group.label.toLowerCase().includes(normalized) ||
          group.objects.length > 0,
      );
  }, [catalog, filter]);

  const database =
    catalog.find((object) => object.kind === "database")?.name ??
    connection?.database ??
    "未连接";
  const schema =
    catalog.find((object) => object.kind === "schema")?.name ?? "public";

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
        <h2>数据库</h2>
        <div className="heading-actions">
          <IconAction
            label="管理数据源"
            icon={<Plug size={16} aria-hidden="true" />}
            onClick={() => setDataSourceOpen(true)}
          />
          <IconAction
            label="管理连接插件"
            icon={<PlugZap size={16} aria-hidden="true" />}
            onClick={() => setPluginManagerOpen(true)}
          />
          <IconAction
            label="新建数据库对象"
            icon={<Plus size={17} aria-hidden="true" />}
            disabled={!connection}
            onClick={() => setNotice("对象编辑将生成可审阅 SQL")}
          />
          <IconAction
            label="刷新数据库对象"
            icon={<RefreshCw size={16} aria-hidden="true" />}
            disabled={!connection}
            onClick={() => void refreshCatalog()}
          />
        </div>
      </div>

      <label className="schema-search">
        <Search size={16} aria-hidden="true" />
        <span className="sr-only">筛选数据库对象</span>
        <input
          type="search"
          aria-label="筛选数据库对象"
          placeholder="筛选对象"
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
        />
        <kbd>Ctrl K</kbd>
      </label>

      <nav className="schema-tree" aria-label="数据库对象">
        <button
          type="button"
          className="tree-row tree-row--connection"
          onClick={() => setDataSourceOpen(true)}
        >
          <ChevronDown size={15} aria-hidden="true" />
          <Server size={16} aria-hidden="true" />
          <span>{connection?.endpoint ?? "选择数据源"}</span>
          <span
            className={`tree-status tree-status--${connectionState}`}
            aria-label={`连接状态：${connectionState}`}
          >
            {connection?.mode === "preview"
              ? "PREVIEW"
              : connectionState === "connected"
                ? "LIVE"
                : connectionState.toUpperCase()}
          </span>
        </button>
        <div className="tree-row tree-row--database">
          <ChevronDown size={15} aria-hidden="true" />
          <Database size={16} aria-hidden="true" />
          <span>{database}</span>
        </div>
        <div className="tree-row tree-row--schema">
          <ChevronDown size={15} aria-hidden="true" />
          <FolderTree size={16} aria-hidden="true" />
          <span>{schema}</span>
        </div>

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
                      selectedObject === object.name ? "tree-row--active" : ""
                    }`}
                    type="button"
                    aria-current={
                      selectedObject === object.name ? "page" : undefined
                    }
                    key={`${object.kind}:${object.schema}:${object.name}`}
                    onClick={() => {
                      setSelectedObject(object.name);
                      setNotice(`${object.schema}.${object.name} · 已选择`);
                    }}
                  >
                    <GroupIcon size={14} aria-hidden="true" />
                    <span>{object.name}</span>
                  </button>
                ))}
            </div>
          );
        })}
        {catalog.length === 0 && (
          <button
            type="button"
            className="schema-empty"
            onClick={() => setDataSourceOpen(true)}
          >
            连接数据源以读取对象
          </button>
        )}
      </nav>

      <div className="schema-footer">
        <span>{catalog.length} 个目录对象</span>
        <IconAction
          label="数据库浏览器更多操作"
          icon={<MoreHorizontal size={17} aria-hidden="true" />}
        />
      </div>
    </aside>
  );
}
