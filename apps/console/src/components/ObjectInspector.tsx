import {
  Bot,
  ChevronRight,
  Copy,
  MoreHorizontal,
  TableProperties,
} from "lucide-react";
import type { DbmsCatalogObject } from "../lib/dbmsClient";
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

const kindLabels: Record<string, string> = {
  database: "DATABASE",
  schema: "SCHEMA",
  table: "TABLE",
  view: "VIEW",
  materializedView: "MATERIALIZED VIEW",
  sequence: "SEQUENCE",
  index: "INDEX",
  constraint: "CONSTRAINT",
  routine: "ROUTINE",
  trigger: "TRIGGER",
};

export function ObjectInspector() {
  const selectedObject = useWorkbenchStore((state) => state.selectedObject);
  const catalogObject = useWorkbenchStore(
    (state) => state.selectedCatalogObject,
  );
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
            <h2>{selectedObject || "未选择对象"}</h2>
            <span>
              {catalogObject
                ? `${catalogObject.schema || "—"} · ${
                    kindLabels[catalogObject.kind] ?? catalogObject.kind.toUpperCase()
                  }`
                : "CATALOG"}
            </span>
          </div>
        </div>
        <IconAction
          label="对象更多操作"
          disabled={!catalogObject}
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

      <div className="inspector-content" role="tabpanel" key={activeTab}>
        {catalogObject ? (
          <InspectorContent activeTab={activeTab} object={catalogObject} />
        ) : (
          <div className="inspector-empty">从数据库浏览器选择对象</div>
        )}
      </div>

      <button className="future-ai-entry" type="button" disabled>
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
  object,
}: {
  activeTab: InspectorTab;
  object: DbmsCatalogObject;
}) {
  const details = asRecord(object.details);
  const ddl = typeof details.ddl === "string" ? details.ddl : null;
  const columns = recordArray(details.columns);
  const constraints = recordArray(details.constraints);
  const indexes = recordArray(details.indexes);

  if (activeTab === "ddl") {
    return (
      <div className="ddl-view">
        <div className="inspector-section-heading">
          <span>定义</span>
          <IconAction
            label="复制 DDL"
            disabled={!ddl}
            icon={<Copy size={15} aria-hidden="true" />}
            onClick={() => {
              if (ddl) void navigator.clipboard?.writeText(ddl);
            }}
          />
        </div>
        {ddl ? <pre>{ddl}</pre> : <InlineEmpty text="服务未提供此对象的 DDL" />}
      </div>
    );
  }

  if (activeTab === "columns") {
    if (columns.length === 0) return <InlineEmpty text="此对象没有列投影" />;
    return (
      <table className="inspector-table">
        <thead>
          <tr>
            <th>列</th>
            <th>类型</th>
            <th>可空</th>
          </tr>
        </thead>
        <tbody>
          {columns.map((column, index) => (
            <tr key={`${displayValue(column.name)}:${index}`}>
              <td>{identifier(column.name)}</td>
              <td>{displayValue(column.dataType)}</td>
              <td>{column.nullable === false ? "否" : "是"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }

  if (activeTab === "constraints") {
    return (
      <ObjectList
        rows={constraints}
        empty="此对象没有约束投影"
        fallbackKind="CONSTRAINT"
      />
    );
  }

  if (activeTab === "indexes") {
    const rows =
      object.kind === "index" && indexes.length === 0 ? [details] : indexes;
    return <ObjectList rows={rows} empty="此对象没有索引投影" fallbackKind="INDEX" />;
  }

  if (activeTab === "statistics") {
    const statistics = asRecord(details.statistics);
    const rows = Object.entries(statistics);
    if (rows.length === 0) {
      return <InlineEmpty text="服务未提供此对象的统计信息" />;
    }
    return (
      <dl className="property-list">
        {rows.map(([label, value]) => (
          <PropertyRow label={label} value={displayValue(value)} key={label} />
        ))}
      </dl>
    );
  }

  return (
    <dl className="property-list">
      <PropertyRow label="名称" value={object.name} />
      <PropertyRow label="Schema" value={object.schema || "—"} />
      <PropertyRow label="类型" value={kindLabels[object.kind] ?? object.kind} />
      <PropertyRow label="父对象" value={object.parent ?? "—"} />
      {typeof details.owner === "string" && (
        <PropertyRow label="所有者" value={identifier(details.owner)} />
      )}
      {typeof details.method === "string" && (
        <PropertyRow label="方法" value={details.method} />
      )}
      {typeof details.mode === "string" && (
        <PropertyRow label="来源" value={details.mode} />
      )}
    </dl>
  );
}

function ObjectList({
  rows,
  empty,
  fallbackKind,
}: {
  rows: Array<Record<string, unknown>>;
  empty: string;
  fallbackKind: string;
}) {
  if (rows.length === 0) return <InlineEmpty text={empty} />;
  return (
    <div className="inspector-list">
      {rows.map((row, index) => (
        <div className="inspector-list-row" key={`${displayValue(row.name)}:${index}`}>
          <span>{identifier(row.name)}</span>
          <span>{displayValue(row.kind ?? row.method ?? fallbackKind)}</span>
          <ChevronRight size={15} aria-hidden="true" />
        </div>
      ))}
    </div>
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

function InlineEmpty({ text }: { text: string }) {
  return <div className="inspector-empty">{text}</div>;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function recordArray(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value) ? value.map(asRecord) : [];
}

function identifier(value: unknown): string {
  const text = displayValue(value);
  return text.replace(/^[uq]:/, "");
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return JSON.stringify(value);
}
