import {
  Cable,
  Check,
  DatabaseZap,
  LoaderCircle,
  PlugZap,
  ShieldCheck,
  TriangleAlert,
  Unplug,
  UserPlus,
  X,
  XCircle,
} from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  connectorDefinitions,
  getConnectorDefinition,
} from "../data/connectors";
import type { ConnectionProbeStageName } from "../lib/dbmsClient";
import {
  useWorkbenchStore,
  type DataSourceValues,
} from "../store/workbench";
import { IconAction } from "./IconAction";

interface DataSourceDialogProps {
  open: boolean;
  onClose: () => void;
  onOpenPluginManager: () => void;
}

const defaults: DataSourceValues = {
  connectorId: "ordadb-native",
  dialect: "postgresql",
  endpoint: "127.0.0.1:54329",
  adminEndpoint: "http://127.0.0.1:9080",
  database: "ordadb",
  credentialId: "ordadb-local",
  username: "ordadb_admin",
  tlsMode: "disable",
};

export function DataSourceDialog({
  open,
  onClose,
  onOpenPluginManager,
}: DataSourceDialogProps) {
  const [values, setValues] = useState<DataSourceValues>(defaults);
  const [submitting, setSubmitting] = useState(false);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const runtimeMode = useWorkbenchStore((state) => state.runtimeMode);
  const connection = useWorkbenchStore((state) => state.connection);
  const activeCredentialId = useWorkbenchStore(
    (state) => state.activeCredentialId,
  );
  const connectionState = useWorkbenchStore((state) => state.connectionState);
  const connectionError = useWorkbenchStore((state) => state.connectionError);
  const connectionProbe = useWorkbenchStore((state) => state.connectionProbe);
  const connectDataSource = useWorkbenchStore(
    (state) => state.connectDataSource,
  );
  const disconnectDataSource = useWorkbenchStore(
    (state) => state.disconnectDataSource,
  );
  const deleteStoredCredential = useWorkbenchStore(
    (state) => state.deleteStoredCredential,
  );
  const bootstrapAdministrator = useWorkbenchStore(
    (state) => state.bootstrapAdministrator,
  );

  useEffect(() => {
    if (!open) return;
    previousFocusRef.current = document.activeElement as HTMLElement;
    window.setTimeout(() => closeButtonRef.current?.focus());
  }, [open]);

  if (!open) return null;

  const selectedConnector = getConnectorDefinition(values.connectorId);
  const native = selectedConnector.id === "ordadb-native";
  const postgresql = selectedConnector.id === "postgresql";
  const preview = runtimeMode === "preview";
  const activeConnector = connection
    ? getConnectorDefinition(connection.connectorId)
    : null;
  const needsBootstrap =
    native &&
    Boolean(connectionProbe?.bootstrapTicket) &&
    connectionProbe?.stages.some(
      (stage) =>
        stage.stage === "initialization" &&
        stage.status === "failed" &&
        stage.error?.sqlState === "55000",
    );

  const close = () => {
    onClose();
    window.setTimeout(() => previousFocusRef.current?.focus());
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    try {
      await connectDataSource(values);
      close();
    } catch {
      // The store owns the structured error rendered below.
    } finally {
      setSubmitting(false);
    }
  };

  const bootstrap = async () => {
    setSubmitting(true);
    try {
      await bootstrapAdministrator(values);
      close();
    } catch {
      // The store owns the structured error rendered below.
    } finally {
      setSubmitting(false);
    }
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
        className="dbms-dialog data-source-dialog"
        role="dialog"
        aria-modal="true"
        aria-label="数据源"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            close();
          }
        }}
      >
        <header className="dbms-dialog-heading">
          <div className="dbms-dialog-title">
            <DatabaseZap size={18} aria-hidden="true" />
            <div>
              <h2>数据源</h2>
              <span>{preview ? "PREVIEW fixture" : "Windows Credential Manager"}</span>
            </div>
          </div>
          <IconAction
            ref={closeButtonRef}
            label="关闭数据源"
            icon={<X size={17} aria-hidden="true" />}
            onClick={close}
          />
        </header>

        {connection && activeConnector && (
          <div className="active-connection" role="status">
            <span className="active-connection-mark" aria-hidden="true">
              <img src={activeConnector.logoUrl} alt="" />
            </span>
            <div>
              <strong>{connection.database}</strong>
              <span>
                {connection.endpoint} · {connection.mode.toUpperCase()}
              </span>
            </div>
            {connection.mode !== "preview" && (
              <button
                className="secondary-action"
                type="button"
                onClick={() => void disconnectDataSource()}
              >
                <Unplug size={15} aria-hidden="true" />
                断开
              </button>
            )}
          </div>
        )}

        {connectionProbe && native && (
          <ol className="connection-probe" aria-label="连接诊断">
            {connectionProbe.stages.map((stage) => (
              <li
                className={`connection-probe--${stage.status}`}
                key={stage.stage}
              >
                {stage.status === "passed" ? (
                  <Check size={14} aria-hidden="true" />
                ) : stage.status === "failed" ? (
                  <XCircle size={14} aria-hidden="true" />
                ) : (
                  <TriangleAlert size={14} aria-hidden="true" />
                )}
                <span>{probeStageLabel(stage.stage)}</span>
                {stage.error && <small>{stage.error.message}</small>}
              </li>
            ))}
          </ol>
        )}

        {needsBootstrap && (
          <div className="bootstrap-guide" role="alert">
            <UserPlus size={17} aria-hidden="true" />
            <div>
              <strong>创建首位管理员</strong>
              <span>仅通过本机受保护通道执行一次。</span>
            </div>
            <button
              className="secondary-action"
              type="button"
              disabled={submitting}
              onClick={() => void bootstrap()}
            >
              初始化并连接
            </button>
          </div>
        )}

        <form className="data-source-form" onSubmit={(event) => void submit(event)}>
          <fieldset className="data-source-picker form-field--wide">
            <legend>数据库</legend>
            <div>
              {connectorDefinitions.map((connector) => (
                <button
                  className={
                    connector.id === selectedConnector.id
                      ? "data-source-choice data-source-choice--active"
                      : "data-source-choice"
                  }
                  type="button"
                  key={connector.id}
                  aria-pressed={connector.id === selectedConnector.id}
                  onClick={() =>
                    setValues((current) => ({
                      ...current,
                      connectorId: connector.id,
                      dialect: connector.sqlDialect,
                      endpoint: connector.defaultEndpoint,
                      adminEndpoint: connector.defaultAdminEndpoint,
                      database: connector.defaultDatabase,
                      tlsMode: connector.defaultTlsMode,
                      credentialId:
                        connector.id === "ordadb-native"
                          ? "ordadb-local"
                          : `${connector.id}-default`,
                    }))
                  }
                >
                  <img src={connector.logoUrl} alt="" />
                  <span>{connector.displayName}</span>
                </button>
              ))}
            </div>
          </fieldset>

          <div className="data-source-identity form-field--wide">
            <img src={selectedConnector.logoUrl} alt="" />
            <div>
              <strong>{selectedConnector.displayName}</strong>
              <span>
                {native ? "OrdaDB 本地服务" : "外部数据库"}
              </span>
            </div>
          </div>

          <label className="form-field">
            <span>{native ? "服务地址" : "主机与端口"}</span>
            <input
              required
              autoComplete="off"
              value={values.endpoint}
              placeholder={selectedConnector.defaultEndpoint || "连接地址"}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  endpoint: event.target.value,
                }))
              }
            />
          </label>

          <label className="form-field">
            <span>数据库</span>
            <input
              value={values.database ?? ""}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  database: event.target.value,
                }))
              }
            />
          </label>

          {native && (
            <label className="form-field form-field--wide">
              <span>管理 API</span>
              <input
                required
                value={values.adminEndpoint ?? ""}
                placeholder="http://127.0.0.1:9080"
                onChange={(event) =>
                  setValues((current) => ({
                    ...current,
                    adminEndpoint: event.target.value,
                  }))
                }
              />
            </label>
          )}

          {postgresql && (
            <label className="form-field form-field--wide">
              <span>TLS</span>
              <select
                value={values.tlsMode}
                onChange={(event) =>
                  setValues((current) => ({
                    ...current,
                    tlsMode: event.target.value as DataSourceValues["tlsMode"],
                  }))
                }
              >
                <option value="disable">Disable</option>
                <option value="prefer">Prefer</option>
                <option value="require">Require</option>
                <option value="verifyCa">Verify CA</option>
                <option value="verifyFull">Verify Full</option>
              </select>
            </label>
          )}

          <label className="form-field">
            <span>用户</span>
            <input
              required
              autoComplete="username"
              value={values.username}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  username: event.target.value,
                }))
              }
            />
          </label>

          <label className="form-field form-field--wide">
            <span>凭据 ID</span>
            <input
              required
              autoComplete="off"
              value={values.credentialId}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  credentialId: event.target.value,
                }))
              }
            />
          </label>

          {connectionError && (
            <div className="structured-error form-field--wide" role="alert">
              <strong>
                {connectionError.sqlState} · {connectionError.message}
              </strong>
              {connectionError.detail && <span>{connectionError.detail}</span>}
              {connectionError.hint && <span>{connectionError.hint}</span>}
              <code>{connectionError.queryId}</code>
            </div>
          )}

          <footer className="dbms-dialog-footer form-field--wide">
            <span className="credential-boundary">
              <ShieldCheck size={15} aria-hidden="true" />
              {preview
                ? "Preview 不保存数据库密码"
                : "密码由 Windows 安全提示直接写入凭据库，不进入网页界面"}
            </span>
            {!native && (
              <button
                className="secondary-action"
                type="button"
                onClick={onOpenPluginManager}
              >
                <PlugZap size={15} aria-hidden="true" />
                连接插件
              </button>
            )}
            {activeCredentialId && !preview && (
              <button
                className="secondary-action"
                type="button"
                onClick={() => void deleteStoredCredential()}
              >
                <Unplug size={15} aria-hidden="true" />
                删除凭据
              </button>
            )}
            <button
              className="primary-action"
              type="submit"
              disabled={submitting || connectionState === "connecting"}
            >
              {submitting || connectionState === "connecting" ? (
                <LoaderCircle
                  className="connector-spinner"
                  size={15}
                  aria-hidden="true"
                />
              ) : (
                <Cable size={15} aria-hidden="true" />
              )}
              {submitting || connectionState === "connecting" ? "连接中" : "连接"}
            </button>
          </footer>
        </form>
      </section>
    </div>
  );
}

function probeStageLabel(stage: ConnectionProbeStageName) {
  switch (stage) {
    case "service":
      return "Windows 服务";
    case "pgPort":
      return "PostgreSQL 端口";
    case "adminApi":
      return "Admin API";
    case "initialization":
      return "初始化";
    case "authentication":
      return "认证";
    case "catalog":
      return "Catalog";
  }
}
