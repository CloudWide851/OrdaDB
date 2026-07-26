import {
  ChevronDown,
  ChevronRight,
  FolderTree,
  MoreHorizontal,
  PlugZap,
  Plus,
  RefreshCw,
  Search,
  Server,
} from "lucide-react";
import { useMemo, useState } from "react";
import {
  databaseObjectGroups,
  databaseSummary,
} from "../data/objects";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

export function SchemaPane() {
  const [filter, setFilter] = useState("");
  const [expandedGroups, setExpandedGroups] = useState(
    () => new Set(["tables", "indexes"]),
  );
  const selectedObject = useWorkbenchStore((state) => state.selectedObject);
  const setSelectedObject = useWorkbenchStore(
    (state) => state.setSelectedObject,
  );
  const setNotice = useWorkbenchStore((state) => state.setNotice);
  const setPluginManagerOpen = useWorkbenchStore(
    (state) => state.setPluginManagerOpen,
  );

  const visibleGroups = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    if (!normalized) return databaseObjectGroups;

    return databaseObjectGroups
      .map((group) => ({
        ...group,
        objects: group.objects.filter((objectName) =>
          objectName.toLowerCase().includes(normalized),
        ),
      }))
      .filter(
        (group) =>
          group.label.toLowerCase().includes(normalized) ||
          group.objects.length > 0,
      );
  }, [filter]);

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
            label="管理连接插件"
            icon={<PlugZap size={16} aria-hidden="true" />}
            onClick={() => setPluginManagerOpen(true)}
          />
          <IconAction
            label="新建数据库对象"
            icon={<Plus size={17} aria-hidden="true" />}
            onClick={() => setNotice("新建对象 · 预览入口")}
          />
          <IconAction
            label="刷新数据库对象"
            icon={<RefreshCw size={16} aria-hidden="true" />}
            onClick={() => setNotice("对象树已刷新 · 预览数据")}
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
        <div className="tree-row tree-row--connection">
          <ChevronDown size={15} aria-hidden="true" />
          <Server size={16} aria-hidden="true" />
          <span>{databaseSummary.connection}</span>
          <span className="tree-status" aria-label="连接状态：预览">
            PREVIEW
          </span>
        </div>
        <div className="tree-row tree-row--database">
          <ChevronDown size={15} aria-hidden="true" />
          <databaseSummary.icon size={16} aria-hidden="true" />
          <span>{databaseSummary.database}</span>
        </div>
        <div className="tree-row tree-row--schema">
          <ChevronDown size={15} aria-hidden="true" />
          <FolderTree size={16} aria-hidden="true" />
          <span>{databaseSummary.schema}</span>
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
                <span className="object-count">{group.count}</span>
              </button>
              {expanded &&
                group.objects.map((objectName) => (
                  <button
                    className={`tree-row tree-row--object ${
                      selectedObject === objectName ? "tree-row--active" : ""
                    }`}
                    type="button"
                    aria-current={
                      selectedObject === objectName ? "page" : undefined
                    }
                    key={objectName}
                    onClick={() => {
                      setSelectedObject(objectName);
                      setNotice(`${objectName} · 对象已选择`);
                    }}
                  >
                    <GroupIcon size={14} aria-hidden="true" />
                    <span>{objectName}</span>
                  </button>
                ))}
            </div>
          );
        })}
      </nav>

      <div className="schema-footer">
        <span>{databaseObjectGroups.length} 类对象</span>
        <IconAction
          label="数据库浏览器更多操作"
          icon={<MoreHorizontal size={17} aria-hidden="true" />}
        />
      </div>
    </aside>
  );
}
