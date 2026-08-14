import { formatSqlForDialect, getSqlDialect } from "../../data/dialects";
import type {
  ConsoleClient,
  ConsoleSettingsV2,
  DocumentLocator,
  OpenSqlDocument,
  RecentFileEntry,
  SqlDocument,
  WorkspaceSessionV1,
  WorkspaceSnapshot,
} from "../../lib/consoleClient";
import { normalizeDbmsError } from "../../lib/dbmsClient";
import type { SqlDialect } from "../../types";
import type { StoreGet, StoreSet } from "./context";
import type { WorkbenchState } from "./types";
import { localError } from "./databaseSupport";
export interface SessionSaveController {
  timer?: ReturnType<typeof setTimeout>;
  autoSaveTimer?: ReturnType<typeof setTimeout>;
}
export function applyConsoleSettings(settings: ConsoleSettingsV2) {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  root.style.setProperty("--font-ui", `${settings.appearance.uiFontSize}px`);
  root.style.setProperty("--font-data", `${settings.appearance.dataFontSize}px`);
  root.style.setProperty("--font-editor", `${settings.editor.fontSize}px`);
  root.style.setProperty("--ui-zoom", `${settings.appearance.zoomPercent / 100}`);
  root.style.setProperty("--editor-font-family", settings.editor.fontFamily);
  root.dataset.density = settings.appearance.density;
  root.dataset.theme = settings.appearance.theme;
  root.dataset.reduceMotion = String(settings.appearance.reduceMotion);
  root.style.colorScheme =
    settings.appearance.theme === "system"
      ? "light dark"
      : settings.appearance.theme;
}

export function toOpenDocument(
  document: Omit<OpenSqlDocument, "savedContent" | "dirty" | "conflict">,
): OpenSqlDocument {
  return {
    ...document,
    savedContent: document.content,
    dirty: false,
    conflict: false,
  };
}

export function prepareDocumentForSave(
  document: OpenSqlDocument,
  settings: ConsoleSettingsV2,
  dialect: SqlDialect,
  allowFormatting: boolean,
) {
  if (!settings.editor.formatOnSave || !allowFormatting) return document;
  const content = formatSqlForDialect(document.content, getSqlDialect(dialect));
  return content === document.content ? document : { ...document, content };
}

export function activateSqlDocument(
  document: SqlDocument,
  set: StoreSet,
  get: StoreGet,
  status: string,
) {
  const key = documentLocatorKey(document.locator);
  const existing = get().documents.find(
    (candidate) => documentLocatorKey(candidate.locator) === key,
  );
  if (existing) {
    set({
      activeDocumentPath: existing.path,
      sql: existing.content,
      recentFiles: addRecentFile(get().recentFiles, document),
      notice: `${existing.name} · 已切换`,
    });
    return;
  }
  const open = toOpenDocument(document);
  set((state) => ({
    documents: [...state.documents, open],
    activeDocumentPath: open.path,
    sql: open.content,
    recentFiles: addRecentFile(state.recentFiles, document),
    notice: `${open.name} · ${status}`,
  }));
}

export async function saveOpenDocument(
  consoleClient: ConsoleClient,
  document: OpenSqlDocument,
  force = false,
) {
  switch (document.locator.kind) {
    case "workspace":
      return consoleClient.saveDocument(
        document.locator.rootPath,
        document,
        force,
      );
    case "external":
      return consoleClient.saveExternalDocument(document, force);
    case "untitled":
      throw localError("55000", "未命名文档需要先选择保存位置");
  }
}

export async function saveNamedDocument(
  path: string,
  force: boolean,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  const document = get().documents.find((candidate) => candidate.path === path);
  if (!document || document.locator.kind === "untitled") return;
  const prepared = prepareDocumentForSave(
    document,
    get().settings,
    get().dialect,
    get().connection?.connectorKind !== "document" &&
      get().connection?.connectorKind !== "keyValue",
  );
  try {
    const saved = await saveOpenDocument(consoleClient, prepared, force);
    set((state) => {
      const active = state.activeDocumentPath === path;
      return {
        documents: state.documents.map((candidate) =>
          candidate.path === path ? toOpenDocument(saved) : candidate,
        ),
        activeDocumentPath: active ? saved.path : state.activeDocumentPath,
        sql: active ? saved.content : state.sql,
        recentFiles: addRecentFile(state.recentFiles, saved),
        notice: `${saved.name} · 已保存`,
      };
    });
    await persistSession(consoleClient, get);
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    set((state) => ({
      documents: state.documents.map((candidate) =>
        candidate.path === path
          ? { ...candidate, conflict: normalized.sqlState === "40001" }
          : candidate,
      ),
      notice: normalized.message,
    }));
    throw normalized;
  }
}

export function addRecentFile(
  recentFiles: RecentFileEntry[],
  document: SqlDocument,
): RecentFileEntry[] {
  const key = documentLocatorKey(document.locator);
  return [
    {
      locator: document.locator,
      name: document.name,
      openedAtMs: Date.now(),
    },
    ...recentFiles.filter(
      (entry) => documentLocatorKey(entry.locator) !== key,
    ),
  ].slice(0, 50);
}

export function documentLocatorKey(locator: DocumentLocator) {
  switch (locator.kind) {
    case "workspace":
      return `workspace:${locator.rootPath.toLocaleLowerCase()}:${locator.path.toLocaleLowerCase()}`;
    case "external":
      return `external:${locator.path.toLocaleLowerCase()}`;
    case "untitled":
      return `untitled:${locator.id}`;
  }
}

export function sameRevision(
  left: OpenSqlDocument["revision"],
  right: OpenSqlDocument["revision"],
) {
  if (!left || !right) return left === right;
  return (
    left.sizeBytes === right.sizeBytes &&
    left.modifiedAtMs === right.modifiedAtMs &&
    left.sha256 === right.sha256
  );
}

export async function activateWorkspace(
  snapshot: WorkspaceSnapshot,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  set((state) => {
    const documents = state.documents.filter(
      (document) => document.locator.kind !== "workspace",
    );
    const active = documents.find(
      (document) => document.path === state.activeDocumentPath,
    );
    return {
      workspace: snapshot,
      documents,
      activeDocumentPath: active?.path ?? null,
      sql: active?.content ?? "",
      recovery: null,
      notice: `${snapshot.rootPath} · 项目已打开`,
    };
  });
  await persistSession(consoleClient, get);
}

export function emptyWorkspaceSession(): WorkspaceSessionV1 {
  return {
    formatVersion: 1,
    rootPath: null,
    activePath: null,
    openDocuments: [],
  };
}

export function workspaceSession(state: WorkbenchState): WorkspaceSessionV1 {
  return {
    formatVersion: 1,
    rootPath: state.workspace?.rootPath ?? null,
    activePath: state.activeDocumentPath,
    openDocuments: state.documents.map((document) => ({
      path: document.path,
      locator: document.locator,
      name: document.name,
      content: document.content,
      baseRevision: document.revision,
    })),
  };
}

export function scheduleSessionSave(
  controller: SessionSaveController,
  consoleClient: ConsoleClient,
  get: StoreGet,
) {
  if (controller.timer) clearTimeout(controller.timer);
  controller.timer = setTimeout(() => {
    controller.timer = undefined;
    void persistSession(consoleClient, get);
  }, 500);
}

export function clearDocumentAutoSave(controller: SessionSaveController) {
  if (!controller.autoSaveTimer) return;
  clearTimeout(controller.autoSaveTimer);
  controller.autoSaveTimer = undefined;
}

export function scheduleDocumentAutoSave(
  controller: SessionSaveController,
  consoleClient: ConsoleClient,
  set: StoreSet,
  get: StoreGet,
) {
  clearDocumentAutoSave(controller);
  const state = get();
  const document = state.documents.find(
    (candidate) => candidate.path === state.activeDocumentPath,
  );
  if (
    state.settings.files.autoSave !== "afterDelay" ||
    !document?.dirty ||
    document.conflict ||
    document.locator.kind === "untitled"
  ) {
    return;
  }
  const activePath = document.path;
  controller.autoSaveTimer = setTimeout(async () => {
    controller.autoSaveTimer = undefined;
    const current = get();
    const pending = current.documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (
      !pending?.dirty ||
      pending.conflict ||
      pending.locator.kind === "untitled"
    ) {
      return;
    }
    await saveNamedDocument(
      activePath,
      false,
      consoleClient,
      set,
      get,
    ).catch(() => undefined);
  }, state.settings.files.autoSaveDelayMs);
}

export async function persistSession(consoleClient: ConsoleClient, get: StoreGet) {
  try {
    await consoleClient.saveSession(workspaceSession(get()));
  } catch (error) {
    const normalized = normalizeDbmsError(error);
    get().setNotice(`草稿恢复状态保存失败 · ${normalized.message}`);
  }
}

export function nextUntitledName(documents: OpenSqlDocument[]) {
  const names = new Set(documents.map((document) => document.name.toLowerCase()));
  for (let sequence = 1; sequence <= 9_999; sequence += 1) {
    const name = `未命名-${sequence}.sql`;
    if (!names.has(name.toLowerCase())) return name;
  }
  return `未命名-${Date.now()}.sql`;
}

export function nextUntitledId(documents: OpenSqlDocument[]) {
  const ids = new Set(
    documents
      .filter(
        (
          document,
        ): document is OpenSqlDocument & {
          locator: Extract<DocumentLocator, { kind: "untitled" }>;
        } => document.locator.kind === "untitled",
      )
      .map((document) => document.locator.id),
  );
  for (let sequence = 1; sequence <= 9_999; sequence += 1) {
    const id = `untitled-${sequence}`;
    if (!ids.has(id)) return id;
  }
  return `untitled-${Date.now()}`;
}

export function renamedPath(path: string, newName: string) {
  const separator = path.lastIndexOf("/");
  return separator < 0 ? newName : `${path.slice(0, separator + 1)}${newName}`;
}

export function replacePathPrefix(path: string, before: string, after: string) {
  if (path === before) return after;
  return path.startsWith(`${before}/`)
    ? `${after}${path.slice(before.length)}`
    : path;
}
