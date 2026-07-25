import {
  Bot,
  ChevronRight,
  Copy,
  MoreHorizontal,
  TableProperties,
} from "lucide-react";
import { useWorkbenchStore } from "../store/workbench";
import type { InspectorTab } from "../types";
import { IconAction } from "./IconAction";

const inspectorTabs: Array<{ id: InspectorTab; label: string }> = [
  { id: "properties", label: "属性" },
  { id: "ddl", label: "DDL" },
  { id: "columns", label: "列" },
  { id: "constraints", label: "约束" },
  { id: "indexes", label: "索引" },
  { id: "statistics", label: "统计" },
];

export function ObjectInspector() {
  const selectedObject = useWorkbenchStore((state) => state.selectedObject);
  const activeTab = useWorkbenchStore((state) => state.activeInspectorTab);
  const setActiveTab = useWorkbenchStore(
    (state) => state.setActiveInspectorTab,
  );

  return (
    <aside className="inspector-pane" aria-label="对象检查器">
      <div className="inspector-heading">
        <div className="inspector-object">
          <TableProperties size={18} aria-hidden="true" />
          <div>
            <h2>{selectedObject}</h2>
            <span>public · TABLE</span>
          </div>
        </div>
        <IconAction
          label="对象更多操作"
          icon={<MoreHorizontal size={17} aria-hidden="true" />}
        />
      </div>

      <div
        className="inspector-tabs"
        role="tablist"
        aria-label="对象详细信息"
      >
        {inspectorTabs.map((tab) => (
          <button
            className={`inspector-tab ${
              activeTab === tab.id ? "inspector-tab--active" : ""
            }`}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="inspector-content" role="tabpanel">
        <InspectorContent activeTab={activeTab} objectName={selectedObject} />
      </div>

      <button className="future-ai-entry" type="button">
        <Bot size={17} aria-hidden="true" />
        <span>AI 助手</span>
        <span className="future-label">后续能力</span>
        <ChevronRight size={15} aria-hidden="true" />
      </button>
    </aside>
  );
}

function InspectorContent({
  activeTab,
  objectName,
}: {
  activeTab: InspectorTab;
  objectName: string;
}) {
  if (activeTab === "ddl") {
    return (
      <div className="ddl-view">
        <div className="inspector-section-heading">
          <span>定义</span>
          <IconAction
            label="复制 DDL"
            icon={<Copy size={15} aria-hidden="true" />}
          />
        </div>
        <pre>{`CREATE TABLE public.${objectName} (
  id BIGINT PRIMARY KEY,
  title TEXT NOT NULL,
  category TEXT,
  updated_at TIMESTAMPTZ
);`}</pre>
      </div>
    );
  }

  if (activeTab === "columns") {
    return (
      <table className="inspector-table">
        <thead>
          <tr>
            <th>列</th>
            <th>类型</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>id</td>
            <td>bigint</td>
          </tr>
          <tr>
            <td>title</td>
            <td>text</td>
          </tr>
          <tr>
            <td>category</td>
            <td>text</td>
          </tr>
          <tr>
            <td>updated_at</td>
            <td>timestamptz</td>
          </tr>
        </tbody>
      </table>
    );
  }

  if (activeTab === "constraints") {
    return (
      <div className="inspector-list">
        <InspectorListRow primary={`${objectName}_pkey`} secondary="PRIMARY KEY" />
        <InspectorListRow primary="title_not_null" secondary="NOT NULL" />
        <InspectorListRow primary="category_check" secondary="CHECK" />
      </div>
    );
  }

  if (activeTab === "indexes") {
    return (
      <div className="inspector-list">
        <InspectorListRow primary={`${objectName}_pkey`} secondary="btree · id" />
        <InspectorListRow
          primary={`${objectName}_search_idx`}
          secondary="hybrid · title"
        />
      </div>
    );
  }

  if (activeTab === "statistics") {
    return (
      <dl className="property-list">
        <PropertyRow label="预估行数" value="128,420" />
        <PropertyRow label="表大小" value="18.4 MB" />
        <PropertyRow label="索引大小" value="7.2 MB" />
        <PropertyRow label="最近分析" value="10:42" />
      </dl>
    );
  }

  return (
    <dl className="property-list">
      <PropertyRow label="名称" value={objectName} />
      <PropertyRow label="Schema" value="public" />
      <PropertyRow label="类型" value="TABLE" />
      <PropertyRow label="所有者" value="ordadb_admin" />
      <PropertyRow label="持久性" value="永久" />
      <PropertyRow label="状态" value="预览对象" />
    </dl>
  );
}

function PropertyRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="property-row">
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function InspectorListRow({
  primary,
  secondary,
}: {
  primary: string;
  secondary: string;
}) {
  return (
    <button className="inspector-list-row" type="button">
      <span>{primary}</span>
      <span>{secondary}</span>
      <ChevronRight size={15} aria-hidden="true" />
    </button>
  );
}
