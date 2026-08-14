import { Tooltip } from "antd";
import {
  Cable,
  CircleCheck,
  CircleOff,
  CloudDownload,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  TriangleAlert,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  connectorPermissionLabels,
  formatConnectorBytes,
  projectConnectorCatalog,
  type ConnectorViewModel,
  type PluginCatalogSnapshot,
  type PluginError,
} from "../data/connectors";
import {
  getPluginManagerClient,
  normalizePluginError,
  type PluginManagerClient,
} from "../lib/pluginManager";
import { usePresence } from "../lib/motion";
import { IconAction } from "./IconAction";

interface ConnectorManagerProps {
  open: boolean;
  onClose: () => void;
}

type ConnectorAction = "install" | "retry" | "update" | "rollback";

const lifecycleLabels: Record<ConnectorViewModel["lifecycle"], string> = {
  unavailable: "不可用",
  available: "可下载",
  downloading: "下载中",
  verifying: "验证签名",
  installing: "安装中",
  installed: "已安装",
  updateAvailable: "可更新",
  failed: "失败",
};

function actionLabel(action: ConnectorAction, connector: ConnectorViewModel) {
  const verb = {
    install: "下载",
    retry: "重试",
    update: "更新",
    rollback: "回滚",
  }[action];
  return `${verb} ${connector.displayName} 连接插件`;
}

function actionIcon(action: ConnectorAction) {
  if (action === "rollback") return <RotateCcw size={15} aria-hidden="true" />;
  if (action === "retry") return <RefreshCw size={15} aria-hidden="true" />;
  return <CloudDownload size={15} aria-hidden="true" />;
}

function primaryAction(
  connector: ConnectorViewModel,
): ConnectorAction | undefined {
  if (connector.lifecycle === "available") return "install";
  if (connector.lifecycle === "unavailable") return "install";
  if (connector.lifecycle === "failed") return "retry";
  if (connector.lifecycle === "updateAvailable") return "update";
  return undefined;
}

function lifecycleIcon(connector: ConnectorViewModel) {
  if (
    connector.lifecycle === "downloading" ||
    connector.lifecycle === "verifying" ||
    connector.lifecycle === "installing"
  ) {
    return <LoaderCircle className="connector-spinner" size={15} aria-hidden="true" />;
  }
  if (connector.lifecycle === "failed") {
    return <TriangleAlert size={15} aria-hidden="true" />;
  }
  if (connector.lifecycle === "unavailable") {
    return <CircleOff size={15} aria-hidden="true" />;
  }
  return <CircleCheck size={15} aria-hidden="true" />;
}

export function ConnectorManager({ open, onClose }: ConnectorManagerProps) {
  const client = useMemo<PluginManagerClient>(() => getPluginManagerClient(), []);
  const [snapshot, setSnapshot] = useState<PluginCatalogSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<PluginError | null>(null);
  const [pendingPluginId, setPendingPluginId] = useState<string | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const presence = usePresence(open);

  const refreshCatalog = useCallback(async () => {
    const next = await client.catalog();
    setSnapshot(next);
    setError(null);
  }, [client]);

  useEffect(() => {
    if (!open) return;
    let active = true;
    previousFocusRef.current = document.activeElement as HTMLElement;
    setLoading(true);
    setError(null);

    void refreshCatalog()
      .catch((catalogError: unknown) => {
        if (active) setError(normalizePluginError(catalogError));
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    let unlisten: (() => void) | undefined;
    void client
      .subscribe(() => {
        if (active) void refreshCatalog().catch(() => undefined);
      })
      .then((dispose) => {
        if (active) unlisten = dispose;
        else dispose();
      })
      .catch((subscriptionError: unknown) => {
        if (active) setError(normalizePluginError(subscriptionError));
      });

    window.setTimeout(() => closeButtonRef.current?.focus());
    return () => {
      active = false;
      unlisten?.();
    };
  }, [client, open, refreshCatalog]);

  if (!presence.mounted) return null;

  const close = () => {
    onClose();
    window.setTimeout(() => previousFocusRef.current?.focus());
  };
  const connectors = snapshot ? projectConnectorCatalog(snapshot) : [];
  const registryConfigured = snapshot?.registry.availability === "configured";

  const runAction = async (
    connector: ConnectorViewModel,
    action: ConnectorAction,
  ) => {
    setPendingPluginId(connector.id);
    setError(null);
    try {
      if (action === "rollback") await client.rollback(connector.id);
      else await client[action](connector.id);
      await refreshCatalog();
    } catch (actionError) {
      setError(normalizePluginError(actionError));
    } finally {
      setPendingPluginId(null);
    }
  };

  const cancel = async (connector: ConnectorViewModel) => {
    if (!connector.operationId) return;
    setPendingPluginId(connector.id);
    try {
      await client.cancel(connector.operationId);
      await refreshCatalog();
    } catch (cancelError) {
      setError(normalizePluginError(cancelError));
    } finally {
      setPendingPluginId(null);
    }
  };

  return (
    <div
      className="connector-manager-backdrop"
      data-motion-presence="panel"
      data-motion-state={presence.phase}
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        className="connector-manager"
        role="dialog"
        aria-modal="true"
        aria-label="连接插件"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            close();
          }
        }}
      >
        <header className="connector-manager-heading">
          <div className="connector-manager-title">
            <Cable size={18} aria-hidden="true" />
            <div>
              <h2>连接插件</h2>
              <span>{client.mode === "preview" ? "Preview 目录" : "官方目录"}</span>
            </div>
          </div>
          <IconAction
            ref={closeButtonRef}
            label="关闭连接插件"
            icon={<X size={17} aria-hidden="true" />}
            onClick={close}
          />
        </header>

        <div
          className={`connector-registry ${
            registryConfigured ? "" : "connector-registry--unavailable"
          }`}
          role="status"
        >
          {registryConfigured ? (
            <ShieldCheck size={16} aria-hidden="true" />
          ) : (
            <TriangleAlert size={16} aria-hidden="true" />
          )}
          <strong>
            {registryConfigured ? "官方签名仓库" : "插件仓库未配置"}
          </strong>
          <span>
            {snapshot?.registry.availability === "notConfigured"
              ? "下载已禁用；不会回退到无签名来源"
              : snapshot?.registry.message ??
                (loading ? "正在读取插件目录" : "等待插件目录")}
          </span>
        </div>

        <div className="connector-list" aria-label="连接类型">
          {loading && connectors.length === 0 && (
            <div className="connector-manager-empty">
              <LoaderCircle className="connector-spinner" size={18} aria-hidden="true" />
              正在读取连接插件
            </div>
          )}
          {!loading &&
            connectors.map((connector) => {
              const action = primaryAction(connector);
              const busy =
                pendingPluginId === connector.id ||
                ["downloading", "verifying", "installing"].includes(
                  connector.lifecycle,
                );
              const progress =
                connector.size > 0
                  ? Math.min(connector.downloadedBytes / connector.size, 1)
                  : 0;

              return (
                <article
                  className="connector-row"
                  data-connector-id={connector.id}
                  key={connector.id}
                >
                  <div className="connector-mark" aria-hidden="true">
                    <img src={connector.logoUrl} alt="" />
                  </div>
                  <div className="connector-identity">
                    <strong>{connector.displayName}</strong>
                    <span>
                      {connector.installedVersion
                        ? `已安装 v${connector.installedVersion} · 最新 v${connector.version}`
                        : `v${connector.version}`}{" "}
                      · {formatConnectorBytes(connector.size)} · {connector.publisher}
                    </span>
                  </div>
                  <div className="connector-details">
                    <span
                      className={`connector-lifecycle connector-lifecycle--${connector.lifecycle}`}
                    >
                      {lifecycleIcon(connector)}
                      {lifecycleLabels[connector.lifecycle]}
                    </span>
                    <span className="connector-permissions">
                      {connector.permissions
                        .map((permission) => connectorPermissionLabels[permission])
                        .join(" · ")}
                    </span>
                    {busy && connector.operationId && (
                      <span className="connector-progress" aria-label="下载进度">
                        <span
                          style={{ transform: `scaleX(${progress})` }}
                          aria-hidden="true"
                        />
                      </span>
                    )}
                    {connector.error && (
                      <span className="connector-error" role="alert">
                        {connector.error.message}
                      </span>
                    )}
                  </div>
                  <div className="connector-actions">
                    {busy && connector.operationId ? (
                      <Tooltip title={`取消 ${connector.displayName} 插件操作`}>
                        <button
                          className="connector-action"
                          type="button"
                          aria-label={`取消 ${connector.displayName} 插件操作`}
                          onClick={() => void cancel(connector)}
                        >
                          <X size={15} aria-hidden="true" />
                          取消
                        </button>
                      </Tooltip>
                    ) : (
                      <>
                        {action && (
                          <Tooltip title={actionLabel(action, connector)}>
                            <button
                              className="connector-action connector-action--primary"
                              type="button"
                              aria-label={actionLabel(action, connector)}
                              disabled={!registryConfigured}
                              onClick={() => void runAction(connector, action)}
                            >
                              {actionIcon(action)}
                              {action === "install"
                                ? "下载"
                                : action === "retry"
                                  ? "重试"
                                  : "更新"}
                            </button>
                          </Tooltip>
                        )}
                        {connector.previousVersion && (
                          <Tooltip title={actionLabel("rollback", connector)}>
                            <button
                              className="connector-action"
                              type="button"
                              aria-label={actionLabel("rollback", connector)}
                              disabled={!registryConfigured}
                              onClick={() =>
                                void runAction(connector, "rollback")
                              }
                            >
                              <RotateCcw size={15} aria-hidden="true" />
                              回滚
                            </button>
                          </Tooltip>
                        )}
                        {!action && !connector.previousVersion && (
                          <span className="connector-action-placeholder">
                            {connector.lifecycle === "installed"
                              ? "可用于数据源"
                              : "不可下载"}
                          </span>
                        )}
                      </>
                    )}
                  </div>
                </article>
              );
            })}
        </div>

        <footer className="connector-manager-footer">
          <span>
            {client.mode === "preview"
              ? "Preview 不执行网络下载或文件写入"
              : "仅安装通过 SHA-256 与 Ed25519 验证的 Windows x64 插件"}
          </span>
          {error && (
            <span className="connector-manager-error" role="alert">
              {error.message}
            </span>
          )}
          <kbd>Esc</kbd>
        </footer>
      </section>
    </div>
  );
}
