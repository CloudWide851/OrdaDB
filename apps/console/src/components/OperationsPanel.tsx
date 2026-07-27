import {
  Activity,
  Archive,
  DatabaseBackup,
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
  const operationView = useWorkbenchStore((state) => state.operationView);
  const connection = useWorkbenchStore((state) => state.connection);
  const monitor = useWorkbenchStore((state) => state.monitor);
  const connectionError = useWorkbenchStore((state) => state.connectionError);
  const refreshMonitor = useWorkbenchStore((state) => state.refreshMonitor);
  const openOperations = useWorkbenchStore((state) => state.openOperations);
  const checkpoint = useWorkbenchStore((state) => state.checkpoint);

  useEffect(() => {
    if (!open) return;
    previousFocusRef.current = document.activeElement as HTMLElement;
    setCheckpointArmed(false);
    window.setTimeout(() => closeButtonRef.current?.focus());
  }, [open]);

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
              onClick={() => void refreshMonitor()}
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

  if (view === "backup") {
    if (!capabilities.backup || !monitor.backups.supported) {
      return (
        <CapabilityEmpty
          title="备份写入未实现"
          detail={monitor.backups.reason || "服务返回 0A000。"}
        />
      );
    }
  }

  if (view === "importExport" && !capabilities.importExport) {
    return (
      <CapabilityEmpty
        title="导入导出不可用"
        detail="当前服务没有声明导入导出能力。"
      />
    );
  }

  if (view === "service") {
    return (
      <OperationSection title="服务">
        <MetricGrid
          values={[
            ["PostgreSQL", monitor.config.pgBind || "—"],
            ["管理 API", monitor.config.adminBind || "—"],
            ["远程 TLS", monitor.config.remoteRequiresTls ? "必须" : "本地"],
          ]}
        />
        {!capabilities.serviceControl && (
          <CapabilityEmpty
            title="服务控制需要 UAC"
            detail="此连接只读展示服务配置，不会模拟启停。"
          />
        )}
      </OperationSection>
    );
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
