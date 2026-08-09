import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { invoke } from "@tauri-apps/api/core";
import {
  compareSemanticVersions,
  pluginConnectorDefinitions,
  type PluginCatalogItem,
  type PluginCatalogSnapshot,
  type PluginError,
  type PluginOperationKind,
  type PluginOperationStarted,
  type PluginProgress,
} from "../data/connectors";
import { isTauriRuntime } from "./tauri";

export interface PluginManagerClient {
  readonly mode: "desktop" | "preview";
  catalog(): Promise<PluginCatalogSnapshot>;
  install(pluginId: string): Promise<PluginOperationStarted>;
  cancel(operationId: string): Promise<void>;
  retry(pluginId: string): Promise<PluginOperationStarted>;
  update(pluginId: string): Promise<PluginOperationStarted>;
  rollback(pluginId: string): Promise<PluginCatalogItem>;
  subscribe(listener: (progress: PluginProgress) => void): Promise<UnlistenFn>;
}

class TauriPluginManagerClient implements PluginManagerClient {
  readonly mode = "desktop";

  catalog() {
    return invoke<PluginCatalogSnapshot>("plugin_catalog");
  }

  install(pluginId: string) {
    return invoke<PluginOperationStarted>("plugin_install", { pluginId });
  }

  cancel(operationId: string) {
    return invoke<void>("plugin_cancel", { operationId });
  }

  retry(pluginId: string) {
    return invoke<PluginOperationStarted>("plugin_retry", { pluginId });
  }

  update(pluginId: string) {
    return invoke<PluginOperationStarted>("plugin_update", { pluginId });
  }

  rollback(pluginId: string) {
    return invoke<PluginCatalogItem>("plugin_rollback", { pluginId });
  }

  subscribe(listener: (progress: PluginProgress) => void) {
    return listen<PluginProgress>("plugin://progress", (event) =>
      listener(event.payload),
    );
  }
}

class PreviewPluginManagerClient implements PluginManagerClient {
  readonly mode = "preview";
  private plugins: PluginCatalogItem[] = [];
  private listeners = new Set<(progress: PluginProgress) => void>();
  private operations = new Map<
    string,
    { pluginId: string; kind: PluginOperationKind; timers: number[] }
  >();
  private sequence = 0;
  private registryAvailability: PluginCatalogSnapshot["registry"]["availability"] =
    "configured";

  constructor() {
    this.reset();
  }

  reset() {
    for (const operation of this.operations.values()) {
      operation.timers.forEach((timer) => window.clearTimeout(timer));
    }
    this.operations.clear();
    this.sequence = 0;
    this.registryAvailability = "configured";
    this.plugins = pluginConnectorDefinitions.map((definition, index) => ({
      id: definition.id,
      displayName: definition.displayName,
      version: index === 3 ? "2.0.0" : "1.0.0",
      dialect: definition.dialect,
      publisher: definition.publisher,
      permissions: [...definition.permissions],
      size: definition.size,
      lifecycle:
        index === 0
          ? "installed"
          : index === 2
            ? "failed"
            : index === 3
              ? "updateAvailable"
              : "available",
      installedVersion: index === 0 ? "1.0.0" : index === 3 ? "1.0.0" : null,
      previousVersion: index === 3 ? "0.9.0" : null,
      operationId: null,
      downloadedBytes: 0,
      error:
        index === 2
          ? previewError("08006", "预览下载已中断，可安全重试")
          : null,
    }));
  }

  async catalog(): Promise<PluginCatalogSnapshot> {
    return {
      registry: {
        availability: this.registryAvailability,
        apiVersion: 3,
        message:
          this.registryAvailability === "configured"
            ? "Preview 插件目录 · 不执行网络或文件操作"
            : "插件仓库未配置",
      },
      plugins: this.plugins.map((plugin) => ({
        ...plugin,
        permissions: [...plugin.permissions],
        error: plugin.error ? { ...plugin.error } : null,
      })),
    };
  }

  install(pluginId: string) {
    return this.start(pluginId, "install");
  }

  retry(pluginId: string) {
    return this.start(pluginId, "retry");
  }

  update(pluginId: string) {
    return this.start(pluginId, "update");
  }

  async cancel(operationId: string): Promise<void> {
    const operation = this.operations.get(operationId);
    if (!operation) throw previewError("42704", "预览操作不存在");
    operation.timers.forEach((timer) => window.clearTimeout(timer));
    this.operations.delete(operationId);
    const plugin = this.plugin(operation.pluginId);
    plugin.lifecycle = plugin.installedVersion
      ? compareSemanticVersions(plugin.version, plugin.installedVersion) > 0
        ? "updateAvailable"
        : "installed"
      : "available";
    plugin.operationId = null;
    plugin.downloadedBytes = 0;
    plugin.error = null;
    this.emit({
      operationId,
      pluginId: plugin.id,
      kind: operation.kind,
      phase: "cancelled",
      downloadedBytes: 0,
      totalBytes: plugin.size,
      error: null,
    });
  }

  async rollback(pluginId: string): Promise<PluginCatalogItem> {
    const plugin = this.plugin(pluginId);
    if (!plugin.previousVersion) {
      throw previewError("55000", "没有可回滚的连接插件版本");
    }
    const active = plugin.installedVersion;
    plugin.installedVersion = plugin.previousVersion;
    plugin.previousVersion = active;
    plugin.lifecycle =
      plugin.installedVersion &&
      compareSemanticVersions(plugin.version, plugin.installedVersion) > 0
        ? "updateAvailable"
        : "installed";
    plugin.error = null;
    return { ...plugin, permissions: [...plugin.permissions] };
  }

  async subscribe(
    listener: (progress: PluginProgress) => void,
  ): Promise<UnlistenFn> {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  setRegistryAvailability(
    availability: PluginCatalogSnapshot["registry"]["availability"],
  ) {
    this.registryAvailability = availability;
  }

  private async start(
    pluginId: string,
    kind: PluginOperationKind,
  ): Promise<PluginOperationStarted> {
    const plugin = this.plugin(pluginId);
    if ([...this.operations.values()].some((item) => item.pluginId === pluginId)) {
      throw previewError("55P03", "已有连接插件操作正在运行");
    }
    const operationId = `preview-${++this.sequence}`;
    plugin.lifecycle = "downloading";
    plugin.operationId = operationId;
    plugin.downloadedBytes = 0;
    plugin.error = null;
    const started = { operationId, pluginId, kind };
    this.emitProgress(started, "resolving", 0);
    const timers = [
      window.setTimeout(() => {
        plugin.downloadedBytes = Math.round(plugin.size * 0.48);
        this.emitProgress(
          started,
          "downloading",
          plugin.downloadedBytes,
        );
      }, 160),
      window.setTimeout(() => {
        plugin.lifecycle = "verifying";
        plugin.downloadedBytes = plugin.size;
        this.emitProgress(started, "verifying", plugin.size);
      }, 420),
      window.setTimeout(() => {
        plugin.lifecycle = "installing";
        this.emitProgress(started, "installing", plugin.size);
      }, 700),
      window.setTimeout(() => {
        const prior = plugin.installedVersion;
        plugin.installedVersion = plugin.version;
        if (prior && prior !== plugin.version) plugin.previousVersion = prior;
        plugin.lifecycle = "installed";
        plugin.operationId = null;
        plugin.downloadedBytes = plugin.size;
        this.operations.delete(operationId);
        this.emitProgress(started, "complete", plugin.size);
      }, 950),
    ];
    this.operations.set(operationId, { pluginId, kind, timers });
    return started;
  }

  private plugin(pluginId: string) {
    const plugin = this.plugins.find((candidate) => candidate.id === pluginId);
    if (!plugin) throw previewError("42704", "连接插件不存在");
    return plugin;
  }

  private emitProgress(
    started: PluginOperationStarted,
    phase: PluginProgress["phase"],
    downloadedBytes: number,
  ) {
    const plugin = this.plugin(started.pluginId);
    this.emit({
      ...started,
      phase,
      downloadedBytes,
      totalBytes: plugin.size,
      error: null,
    });
  }

  private emit(progress: PluginProgress) {
    this.listeners.forEach((listener) => listener(progress));
  }
}

function previewError(sqlState: string, message: string): PluginError {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: `preview-${sqlState}`,
  };
}

const tauriClient = new TauriPluginManagerClient();
const previewClient = new PreviewPluginManagerClient();

export function getPluginManagerClient(): PluginManagerClient {
  return isTauriRuntime() ? tauriClient : previewClient;
}

export function resetPreviewPluginManagerForTests() {
  previewClient.reset();
}

export function setPreviewRegistryAvailabilityForTests(
  availability: PluginCatalogSnapshot["registry"]["availability"],
) {
  previewClient.setRegistryAvailability(availability);
}

export function normalizePluginError(error: unknown): PluginError {
  if (
    typeof error === "object" &&
    error !== null &&
    "sqlState" in error &&
    "message" in error
  ) {
    const candidate = error as Partial<PluginError>;
    return {
      sqlState: String(candidate.sqlState),
      message: String(candidate.message),
      detail: candidate.detail ? String(candidate.detail) : null,
      hint: candidate.hint ? String(candidate.hint) : null,
      position:
        typeof candidate.position === "number" ? candidate.position : null,
      queryId: candidate.queryId ? String(candidate.queryId) : "connector-ui",
    };
  }
  return previewError(
    "XX000",
    error instanceof Error ? error.message : String(error),
  );
}
