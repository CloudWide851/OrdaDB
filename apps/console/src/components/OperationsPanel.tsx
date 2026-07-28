import {
  Activity,
  Archive,
  CircleStop,
  DatabaseBackup,
  FileInput,
  FileOutput,
  HardDrive,
  KeyRound,
  LoaderCircle,
  LockKeyhole,
  RefreshCw,
  ServerCog,
  ShieldAlert,
  Users,
  X,
} from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import type {
  DbmsOperationKind,
  DbmsOperationRecord,
  DbmsServiceStatus,
  DbmsTransferFormat,
} from "../lib/dbmsClient";
import {
  useWorkbenchStore,
  type OperationView,
} from "../store/workbench";
import { IconAction } from "./IconAction";

interface OperationsPanelProps {
  open: boolean;
  onClose: () => void;
}

const operationTabs: Array<{
  id: OperationView;
  label: string;
  icon: typeof Activity;
}> = [
  { id: "sessions", label: "会话", icon: Users },
  { id: "locks", label: "锁", icon: LockKeyhole },
  { id: "transactions", label: "事务", icon: Activity },
  { id: "roles", label: "角色", icon: KeyRound },
  { id: "wal", label: "WAL", icon: HardDrive },
  { id: "backup", label: "备份", icon: DatabaseBackup },
  { id: "importExport", label: "导入导出", icon: Archive },
  { id: "service", label: "服务", icon: ServerCog },
];

export function OperationsPanel({ open, onClose }: OperationsPanelProps) {
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const [checkpointArmed, setCheckpointArmed] = useState(false);
  const [armedAction, setArmedAction] = useState<DbmsOperationKind | null>(null);
  const [operationPath, setOperationPath] = useState("ordadb-backup.ordbak");
  const [schema, setSchema] = useState("public");
  const [table, setTable] = useState("documents");
  const [format, setFormat] = useState<DbmsTransferFormat>("csv");
  const operationView = useWorkbenchStore((state) => state.operationView);
  const connection = useWorkbenchStore((state) => state.connection);
  const monitor = useWorkbenchStore((state) => state.monitor);
  const operations = useWorkbenchStore((state) => state.operations);
  const serviceStatus = useWorkbenchStore((state) => state.serviceStatus);
  const administrationBusy = useWorkbenchStore(
    (state) => state.administrationBusy,
  );
  const connectionError = useWorkbenchStore((state) => state.connectionError);
  const refreshMonitor = useWorkbenchStore((state) => state.refreshMonitor);
  const refreshAdministration = useWorkbenchStore(
    (state) => state.refreshAdministration,
  );
  const startAdministrationOperation = useWorkbenchStore(
    (state) => state.startAdministrationOperation,
  );
  const cancelAdministrationOperation = useWorkbenchStore(
    (state) => state.cancelAdministrationOperation,
  );
  const openOperations = useWorkbenchStore((state) => state.openOperations);
  const checkpoint = useWorkbenchStore((state) => state.checkpoint);

  useEffect(() => {
    if (!open) return;
    previousFocusRef.current = document.activeElement as HTMLElement;
    setCheckpointArmed(false);
    setArmedAction(null);
    window.setTimeout(() => closeButtonRef.current?.focus());
  }, [open]);

  useEffect(() => {
    if (
      !open ||
      !operations.some(
        (operation) =>
          operation.state === "queued" || operation.state === "running",
      )
    ) {
      return;
    }
    const interval = window.setInterval(() => {
      void refreshAdministration();
    }, 750);
    return () => window.clearInterval(interval);
  }, [open, operations, refreshAdministration]);

  if (!open) return null;

  const close = () => {
    onClose();
    window.setTimeout(() => previousFocusRef.current?.focus());
  };

  return (
    <div
      className="dbms-dialog-backdrop"
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        className="dbms-dialog operations-panel"
        role="dialog"
        aria-modal="true"
        aria-label="数据库运维"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            close();
          }
        }}
      >
        <header className="dbms-dialog-heading">
          <div className="dbms-dialog-title">
            <Activity size={18} aria-hidden="true" />
            <div>
              <h2>数据库运维</h2>
              <span>
                {connection
                  ? `${connection.database} · ${connection.mode.toUpperCase()}`
                  : "未连接"}
              </span>
            </div>
          </div>
          <div className="heading-actions">
            <IconAction
              label="刷新运维状态"
              icon={<RefreshCw size={16} aria-hidden="true" />}
              disabled={!connection}
              onClick={() => {
                void Promise.all([refreshMonitor(), refreshAdministration()]);
              }}
            />
            <IconAction
              ref={closeButtonRef}
              label="关闭数据库运维"
              icon={<X size={17} aria-hidden="true" />}
              onClick={close}
            />
          </div>
        </header>

        <div className="operations-layout">
          <nav className="operations-nav" aria-label="运维视图">
            {operationTabs.map((tab) => {
              const TabIcon = tab.icon;
              return (
                <button
                  type="button"
                  className={operationView === tab.id ? "active" : ""}
                  aria-current={operationView === tab.id ? "page" : undefined}
                  onClick={() => void openOperations(tab.id)}
                  key={tab.id}
                >
                  <TabIcon size={15} aria-hidden="true" />
                  {tab.label}
                </button>
              );
            })}
          </nav>

          <div className="operations-content" aria-live="polite">
            {!connection ? (
              <CapabilityEmpty
                title="需要数据源"
                detail="连接后读取真实会话、锁、存储与 WAL 状态。"
              />
            ) : !monitor ? (
              <div className="operations-loading">
                <LoaderCircle size={18} aria-hidden="true" />
                正在读取运维状态
              </div>
            ) : (
              <OperationContent
                view={operationView}
                connection={connection}
                monitor={monitor}
                checkpointArmed={checkpointArmed}
                administrationContent={
                  <AdministrationContent
                    view={operationView}
                    connectionMode={connection.mode}
                    backupEnabled={connection.capabilities.backup}
                    importExportEnabled={connection.capabilities.importExport}
                    operations={operations}
                    serviceStatus={serviceStatus}
                    busy={administrationBusy}
                    operationPath={operationPath}
                    schema={schema}
                    table={table}
                    format={format}
                    armedAction={armedAction}
                    onPathChange={setOperationPath}
                    onSchemaChange={setSchema}
                    onTableChange={setTable}
                    onFormatChange={setFormat}
                    onStart={(kind) => {
                      const destructive = kind === "restore" || kind === "import";
                      if (destructive && armedAction !== kind) {
                        setArmedAction(kind);
                        return;
                      }
                      setArmedAction(null);
                      void startAdministrationOperation({
                        kind,
                        path: operationPath,
                        ...(kind === "import" || kind === "export"
                          ? { schema, table, format }
                          : {}),
                      });
                    }}
                    onCancel={(operationId) =>
                      void cancelAdministrationOperation(operationId)
                    }
                  />
                }
                onCheckpoint={() => {
                  if (!checkpointArmed) {
                    setCheckpointArmed(true);
                    return;
                  }
                  setCheckpointArmed(false);
                  void checkpoint();
                }}
              />
            )}
            {connectionError && (
              <div className="structured-error" role="alert">
                <strong>
                  {connectionError.sqlState} · {connectionError.message}
                </strong>
                {connectionError.detail && <span>{connectionError.detail}</span>}
                {connectionError.hint && <span>{connectionError.hint}</span>}
                <code>{connectionError.queryId}</code>
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}

function OperationContent({
  view,
  connection,
  monitor,
  checkpointArmed,
  administrationContent,
  onCheckpoint,
}: {
  view: OperationView;
  connection: NonNullable<
    ReturnType<typeof useWorkbenchStore.getState>["connection"]
  >;
  monitor: NonNullable<
    ReturnType<typeof useWorkbenchStore.getState>["monitor"]
  >;
  checkpointArmed: boolean;
  administrationContent: ReactNode;
  onCheckpoint: () => void;
}) {
  const capabilities = connection.capabilities;

  if (view === "sessions") {
    if (!capabilities.sessions) {
      return <CapabilityEmpty title="会话不可用" detail="当前连接未提供会话监控。" />;
    }
    return (
      <OperationSection title="会话" count={monitor.sessions.length}>
        <table className="operations-table">
          <thead>
            <tr>
              <th>PID</th>
              <th>用户</th>
              <th>数据库</th>
              <th>客户端</th>
            </tr>
          </thead>
          <tbody>
            {monitor.sessions.map((session) => (
              <tr key={session.processId}>
                <td>{session.processId}</td>
                <td>{session.user}</td>
                <td>{session.database}</td>
                <td>{session.applicationName ?? session.remoteAddress}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {monitor.sessions.length === 0 && <InlineEmpty text="当前没有活动会话" />}
      </OperationSection>
    );
  }

  if (view === "locks") {
    if (!capabilities.locks) {
      return <CapabilityEmpty title="锁监控不可用" detail="当前连接未提供锁状态。" />;
    }
    return (
      <OperationSection title="锁">
        <MetricGrid
          values={[
            ["写入模型", monitor.locks.singleWriter ? "单写" : "多写"],
            ["活动锁", String(monitor.locks.activeLocks.length)],
          ]}
        />
        {monitor.locks.activeLocks.map((lock) => (
          <div className="operation-row" key={lock}>
            <LockKeyhole size={15} aria-hidden="true" />
            <span>{lock}</span>
          </div>
        ))}
      </OperationSection>
    );
  }

  if (view === "transactions") {
    if (!capabilities.transactions) {
      return <CapabilityEmpty title="事务不可用" detail="当前连接未提供事务控制。" />;
    }
    return (
      <OperationSection title="事务与查询" count={monitor.queries.length}>
        <MetricGrid
          values={[
            ["活动会话", String(monitor.metrics.activeSessions)],
            ["活动查询", String(monitor.metrics.activeQueries)],
          ]}
        />
        {monitor.queries.map((query) => (
          <div className="operation-row operation-row--query" key={query.queryId}>
            <code>{query.queryId}</code>
            <span>{query.sql}</span>
            <span>{query.rowsProcessed} 行</span>
          </div>
        ))}
      </OperationSection>
    );
  }

  if (view === "wal") {
    if (!capabilities.wal) {
      return <CapabilityEmpty title="WAL 不可用" detail="当前连接未提供 WAL 状态。" />;
    }
    return (
      <OperationSection title="WAL 与存储">
        <MetricGrid
          values={[
            ["持久 LSN", String(monitor.wal.durableLsn ?? "—")],
            ["脏页", String(monitor.storage.dirtyPageCount)],
            ["提交", String(monitor.wal.commitsSinceCheckpoint)],
            ["世代", String(monitor.storage.generation)],
          ]}
        />
        <button
          className={checkpointArmed ? "danger-action" : "primary-action"}
          type="button"
          disabled={!capabilities.checkpoint}
          onClick={onCheckpoint}
        >
          <HardDrive size={15} aria-hidden="true" />
          {checkpointArmed ? "确认执行检查点" : "执行检查点"}
        </button>
      </OperationSection>
    );
  }

  if (
    view === "backup" ||
    view === "importExport" ||
    view === "service"
  ) {
    return administrationContent;
  }

  return (
    <CapabilityEmpty
      title={view === "roles" ? "角色目录未暴露" : "能力未提供"}
      detail={
        view === "roles"
          ? "服务尚未提供可枚举角色的管理端点。"
          : "当前连接未声明此操作能力。"
      }
    />
  );
}

function AdministrationContent({
  view,
  connectionMode,
  backupEnabled,
  importExportEnabled,
  operations,
  serviceStatus,
  busy,
  operationPath,
  schema,
  table,
  format,
  armedAction,
  onPathChange,
  onSchemaChange,
  onTableChange,
  onFormatChange,
  onStart,
  onCancel,
}: {
  view: OperationView;
  connectionMode: "native" | "plugin" | "preview";
  backupEnabled: boolean;
  importExportEnabled: boolean;
  operations: DbmsOperationRecord[];
  serviceStatus: DbmsServiceStatus | null;
  busy: boolean;
  operationPath: string;
  schema: string;
  table: string;
  format: DbmsTransferFormat;
  armedAction: DbmsOperationKind | null;
  onPathChange: (path: string) => void;
  onSchemaChange: (schema: string) => void;
  onTableChange: (table: string) => void;
  onFormatChange: (format: DbmsTransferFormat) => void;
  onStart: (kind: DbmsOperationKind) => void;
  onCancel: (operationId: string) => void;
}) {
  const fixtureSuffix = connectionMode === "preview" ? " · Preview fixture" : "";
  if (view === "backup") {
    if (!backupEnabled) {
      return (
        <CapabilityEmpty
          title="备份不可用"
          detail="当前连接未提供 OrdaDB 逻辑归档能力。"
        />
      );
    }
    return (
      <OperationSection title={`逻辑备份与恢复${fixtureSuffix}`}>
        <div className="operation-form operation-form--backup">
          <label>
            归档文件
            <input
              aria-label="逻辑归档文件"
              value={operationPath}
              onChange={(event) => onPathChange(event.target.value)}
            />
          </label>
          <div className="operation-form-actions">
            <button
              type="button"
              className="primary-action"
              disabled={busy || !operationPath.trim()}
              onClick={() => onStart("backup")}
            >
              <DatabaseBackup size={15} aria-hidden="true" />
              创建备份
            </button>
            <button
              type="button"
              className={armedAction === "restore" ? "danger-action" : "secondary-action"}
              disabled={busy || !operationPath.trim()}
              onClick={() => onStart("restore")}
            >
              <FileInput size={15} aria-hidden="true" />
              {armedAction === "restore" ? "确认恢复并替换" : "恢复归档"}
            </button>
          </div>
        </div>
        <OperationList
          operations={operations.filter(
            (operation) =>
              operation.kind === "backup" || operation.kind === "restore",
          )}
          onCancel={onCancel}
        />
      </OperationSection>
    );
  }

  if (view === "importExport") {
    if (!importExportEnabled) {
      return (
        <CapabilityEmpty
          title="导入导出不可用"
          detail="当前连接未提供表级文件交换能力。"
        />
      );
    }
    return (
      <OperationSection title={`表数据交换${fixtureSuffix}`}>
        <div className="operation-form operation-form--transfer">
          <label>
            Schema
            <input
              aria-label="导入导出 Schema"
              value={schema}
              onChange={(event) => onSchemaChange(event.target.value)}
            />
          </label>
          <label>
            表
            <input
              aria-label="导入导出表"
              value={table}
              onChange={(event) => onTableChange(event.target.value)}
            />
          </label>
          <label>
            格式
            <select
              aria-label="导入导出格式"
              value={format}
              onChange={(event) =>
                onFormatChange(event.target.value as DbmsTransferFormat)
              }
            >
              <option value="csv">CSV</option>
              <option value="jsonLines">JSON Lines</option>
            </select>
          </label>
          <label className="operation-form-path">
            文件
            <input
              aria-label="导入导出文件"
              value={operationPath}
              onChange={(event) => onPathChange(event.target.value)}
            />
          </label>
          <div className="operation-form-actions">
            <button
              type="button"
              className={armedAction === "import" ? "danger-action" : "secondary-action"}
              disabled={busy || !schema.trim() || !table.trim() || !operationPath.trim()}
              onClick={() => onStart("import")}
            >
              <FileInput size={15} aria-hidden="true" />
              {armedAction === "import" ? "确认导入" : "导入"}
            </button>
            <button
              type="button"
              className="primary-action"
              disabled={busy || !schema.trim() || !table.trim() || !operationPath.trim()}
              onClick={() => onStart("export")}
            >
              <FileOutput size={15} aria-hidden="true" />
              导出
            </button>
          </div>
        </div>
        <OperationList
          operations={operations.filter(
            (operation) =>
              operation.kind === "import" || operation.kind === "export",
          )}
          onCancel={onCancel}
        />
      </OperationSection>
    );
  }

  if (view === "service") {
    if (!serviceStatus) {
      return <CapabilityEmpty title="服务状态不可用" detail="当前连接未返回服务状态。" />;
    }
    return (
      <OperationSection title={`Windows 服务${fixtureSuffix}`}>
        <MetricGrid
          values={[
            ["名称", serviceStatus.name],
            ["进程", serviceStatus.processRunning ? "运行中" : "未运行"],
            ["Windows SCM", serviceStatus.windowsServiceSupported ? "支持" : "不可用"],
            ["数据目录", serviceStatus.dataDir || "—"],
            ["操作目录", serviceStatus.operationsRoot || "—"],
          ]}
        />
        <CapabilityEmpty
          title="服务生命周期由安装器与 CLI 管理"
          detail="Console 只读展示状态；启停与卸载需要管理员权限。"
        />
      </OperationSection>
    );
  }

  return null;
}

function OperationList({
  operations,
  onCancel,
}: {
  operations: DbmsOperationRecord[];
  onCancel: (operationId: string) => void;
}) {
  if (operations.length === 0) {
    return <InlineEmpty text="暂无作业" />;
  }
  return (
    <div className="operation-jobs" aria-label="数据库作业">
      {operations.map((operation) => {
        const active =
          operation.state === "queued" || operation.state === "running";
        return (
          <div className="operation-job" key={operation.operationId}>
            <div>
              <strong>{operationKindLabel(operation.kind)}</strong>
              <span>{operation.path}</span>
            </div>
            <span className={`operation-state operation-state--${operation.state}`}>
              {operationStateLabel(operation.state)}
            </span>
            <span>{operation.rows === null ? "—" : `${operation.rows} 行`}</span>
            {active ? (
              <button
                type="button"
                className="secondary-action"
                onClick={() => onCancel(operation.operationId)}
              >
                <CircleStop size={14} aria-hidden="true" />
                取消
              </button>
            ) : (
              <span>{formatBytes(operation.bytes)}</span>
            )}
            {operation.error && (
              <div className="structured-error operation-job-error" role="alert">
                <strong>
                  {operation.error.sqlState} · {operation.error.message}
                </strong>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function operationKindLabel(kind: DbmsOperationKind) {
  return {
    backup: "备份",
    restore: "恢复",
    import: "导入",
    export: "导出",
  }[kind];
}

function operationStateLabel(state: DbmsOperationRecord["state"]) {
  return {
    queued: "排队",
    running: "运行中",
    succeeded: "完成",
    failed: "失败",
    cancelled: "已取消",
  }[state];
}

function formatBytes(bytes: number | null) {
  if (bytes === null) return "—";
  if (bytes < 1024) return `${bytes} B`;
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

function OperationSection({
  title,
  count,
  children,
}: {
  title: string;
  count?: number;
  children: ReactNode;
}) {
  return (
    <section className="operation-section">
      <header>
        <h3>{title}</h3>
        {count !== undefined && <span>{count}</span>}
      </header>
      {children}
    </section>
  );
}

function MetricGrid({ values }: { values: Array<[string, string]> }) {
  return (
    <dl className="operation-metrics">
      {values.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function CapabilityEmpty({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="capability-empty">
      <ShieldAlert size={22} aria-hidden="true" />
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}

function InlineEmpty({ text }: { text: string }) {
  return <div className="inline-empty">{text}</div>;
}
