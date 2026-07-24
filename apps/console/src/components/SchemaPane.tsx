import {
  ChevronDown,
  Columns3,
  Database,
  MoreHorizontal,
  Plus,
  RefreshCw,
  Search,
  Table2,
} from "lucide-react";
import { schemaGroups } from "../data/preview";
import { IconAction } from "./IconAction";

export function SchemaPane() {
  return (
    <aside className="schema-pane" aria-label="Schema 浏览器">
      <div className="pane-heading">
        <div>
          <span className="eyebrow">LOCALHOST:5432</span>
          <h2>Schema</h2>
        </div>
        <div className="heading-actions">
          <IconAction
            label="新建对象"
            icon={<Plus size={17} aria-hidden="true" />}
          />
          <IconAction
            label="刷新 Schema"
            icon={<RefreshCw size={16} aria-hidden="true" />}
          />
        </div>
      </div>

      <label className="schema-search">
        <Search size={16} aria-hidden="true" />
        <span className="sr-only">筛选数据库对象</span>
        <input type="search" placeholder="筛选对象" />
        <kbd>Ctrl K</kbd>
      </label>

      <nav className="schema-tree" aria-label="数据库对象">
        <div className="database-node">
          <div className="tree-row tree-row--database">
            <ChevronDown size={16} aria-hidden="true" />
            <Database size={17} aria-hidden="true" />
            <span>ordadb_local</span>
            <span className="tree-status">READY</span>
          </div>

          {schemaGroups.map((group) => (
            <div className="schema-group" key={group.name}>
              <div className="tree-row tree-row--schema">
                <ChevronDown size={15} aria-hidden="true" />
                <Columns3 size={16} aria-hidden="true" />
                <span>{group.name}</span>
              </div>
              {group.tables.map((table, index) => (
                <button
                  className={`tree-row tree-row--table ${
                    group.name === "public" && index === 0
                      ? "tree-row--active"
                      : ""
                  }`}
                  key={table.name}
                  type="button"
                  aria-current={
                    group.name === "public" && index === 0 ? "page" : undefined
                  }
                >
                  <Table2 size={15} aria-hidden="true" />
                  <span>{table.name}</span>
                  <span className="object-count">{table.count}</span>
                </button>
              ))}
            </div>
          ))}
        </div>
      </nav>

      <div className="schema-footer">
        <span>2 schema · 5 对象</span>
        <IconAction
          label="Schema 更多操作"
          icon={<MoreHorizontal size={17} aria-hidden="true" />}
        />
      </div>
    </aside>
  );
}
