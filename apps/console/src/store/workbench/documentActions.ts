import { formatSqlForDialect, getSqlDialect } from "../../data/dialects";
import type { OpenSqlDocument } from "../../lib/consoleClient";
import { normalizeDbmsError } from "../../lib/dbmsClient";
import type { WorkbenchActionContext } from "./context";
import {
  activateSqlDocument,
  activateWorkspace,
  addRecentFile,
  applyConsoleSettings,
  clearDocumentAutoSave,
  emptyWorkspaceSession,
  nextUntitledId,
  nextUntitledName,
  persistSession,
  prepareDocumentForSave,
  renamedPath,
  replacePathPrefix,
  sameRevision,
  saveNamedDocument,
  saveOpenDocument,
  scheduleDocumentAutoSave,
  scheduleSessionSave,
  toOpenDocument,
} from "./documentSupport";
import type { WorkbenchState } from "./types";

export function createDocumentActions({
  consoleClient,
  get,
  sessionSaveController,
  set,
}: WorkbenchActionContext) {
  return {  setSql: (sql) => {
    set((state) => ({
      sql,
      documents: state.documents.map((document) =>
        document.path === state.activeDocumentPath
          ? {
              ...document,
              content: sql,
              dirty:
                document.locator.kind === "untitled" ||
                sql !== document.savedContent,
            }
          : document,
      ),
    }));
    scheduleSessionSave(sessionSaveController, consoleClient, get);
    scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
  },
  setSettingsOpen: (settingsOpen) => set({ settingsOpen }),
  saveSettings: async (settings) => {
    try {
      const saved = await consoleClient.saveSettings(settings);
      applyConsoleSettings(saved);
      set({
        settings: saved,
        settingsOpen: false,
        notice: "设置已保存",
      });
      scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  openWorkspace: async () => {
    try {
      const snapshot = await consoleClient.pickWorkspace();
      if (!snapshot) return;
      await activateWorkspace(snapshot, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  openWorkspacePath: async (rootPath) => {
    try {
      const snapshot = await consoleClient.openWorkspace(rootPath);
      await activateWorkspace(snapshot, consoleClient, set, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  openFile: async () => {
    try {
      const document = await consoleClient.pickDocument();
      if (!document) return;
      activateSqlDocument(document, set, get, "已打开");
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  openExternalFiles: async (paths) => {
    for (const path of paths) {
      if (!path.toLowerCase().endsWith(".sql")) continue;
      try {
        const document = await consoleClient.openExternalDocument(path);
        activateSqlDocument(document, set, get, "已打开");
      } catch (error) {
        const normalized = normalizeDbmsError(error);
        set({ notice: normalized.message });
      }
    }
    await persistSession(consoleClient, get);
  },
  openRecentFile: async (entry) => {
    try {
      if (entry.locator.kind === "workspace") {
        if (get().workspace?.rootPath !== entry.locator.rootPath) {
          const snapshot = await consoleClient.openWorkspace(
            entry.locator.rootPath,
          );
          await activateWorkspace(snapshot, consoleClient, set, get);
        }
        await get().openDocument(entry.locator.path);
      } else {
        const document = await consoleClient.openExternalDocument(
          entry.locator.path,
        );
        activateSqlDocument(document, set, get, "已打开");
        await persistSession(consoleClient, get);
      }
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  restoreRecovery: async () => {
    const recovery = get().recovery;
    if (!recovery) return;
    try {
      const snapshot = recovery.rootPath
        ? await consoleClient.openWorkspace(recovery.rootPath)
        : null;
      const documents: OpenSqlDocument[] = [];
      for (const draft of recovery.openDocuments) {
        const locator =
          draft.locator ??
          (recovery.rootPath
            ? {
                kind: "workspace" as const,
                rootPath: recovery.rootPath,
                path: draft.path,
              }
            : null);
        if (!locator) continue;
        if (locator.kind === "untitled") {
          documents.push({
            locator,
            path: draft.path,
            name: draft.name ?? nextUntitledName(documents),
            content: draft.content,
            revision: null,
            savedContent: "",
            dirty: true,
            conflict: false,
          });
          continue;
        }
        const current =
          locator.kind === "workspace"
            ? await consoleClient.openDocument(locator.rootPath, locator.path)
            : await consoleClient.openExternalDocument(locator.path);
        documents.push({
          ...current,
          content: draft.content,
          savedContent: current.content,
          dirty: draft.content !== current.content,
          conflict:
            draft.baseRevision !== null &&
            !sameRevision(draft.baseRevision, current.revision),
        });
      }
      const activePath =
        recovery.activePath &&
        documents.some((document) => document.path === recovery.activePath)
          ? recovery.activePath
          : documents[0]?.path ?? null;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath: activePath,
        sql:
          documents.find((document) => document.path === activePath)?.content ??
          "",
        recovery: null,
        notice: `已恢复 ${documents.length} 个 SQL 草稿`,
      });
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  discardRecovery: async () => {
    set({ recovery: null });
    await consoleClient.saveSession(emptyWorkspaceSession());
    set({ notice: "已丢弃上次草稿" });
  },
  openDocument: async (path) => {
    const workspace = get().workspace;
    if (!workspace) return;
    const existing = get().documents.find((document) => document.path === path);
    if (existing) {
      get().activateDocument(path);
      return;
    }
    try {
      const document = await consoleClient.openDocument(workspace.rootPath, path);
      activateSqlDocument(document, set, get, "已打开");
      scheduleSessionSave(sessionSaveController, consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  createDocument: async (parentPath = "") => {
    void parentPath;
    const id = nextUntitledId(get().documents);
    const name = nextUntitledName(get().documents);
    const document: OpenSqlDocument = {
      locator: { kind: "untitled", id },
      path: `untitled:${id}`,
      name,
      content: "",
      revision: null,
      savedContent: "",
      dirty: true,
      conflict: false,
    };
    set((state) => ({
      documents: [...state.documents, document],
      activeDocumentPath: document.path,
      sql: "",
      notice: `${name} · 首次保存时选择位置`,
    }));
    scheduleSessionSave(sessionSaveController, consoleClient, get);
    scheduleDocumentAutoSave(sessionSaveController, consoleClient, set, get);
  },
  activateDocument: (path) => {
    const document = get().documents.find((candidate) => candidate.path === path);
    if (!document) return;
    set({
      activeDocumentPath: path,
      sql: document.content,
      notice: document.dirty ? `${document.name} · 未保存` : document.name,
    });
    scheduleSessionSave(sessionSaveController, consoleClient, get);
  },
  closeDocument: async (path) => {
    const documents = get().documents.filter(
      (document) => document.path !== path,
    );
    const activePath =
      get().activeDocumentPath === path
        ? documents.at(-1)?.path ?? null
        : get().activeDocumentPath;
    set({
      documents,
      activeDocumentPath: activePath,
      sql:
        documents.find((document) => document.path === activePath)?.content ?? "",
      notice: "SQL 文件已关闭",
    });
    await persistSession(consoleClient, get);
  },
  reloadActiveDocument: async () => {
    const activePath = get().activeDocumentPath;
    const active = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!active || active.locator.kind === "untitled") return;
    try {
      const document =
        active.locator.kind === "workspace"
          ? await consoleClient.openDocument(
              active.locator.rootPath,
              active.locator.path,
            )
          : await consoleClient.openExternalDocument(active.locator.path);
      const reloaded = toOpenDocument(document);
      set((state) => ({
        documents: state.documents.map((candidate) =>
          candidate.path === activePath ? reloaded : candidate,
        ),
        sql: reloaded.content,
        notice: `${reloaded.name} · 已从磁盘重新加载`,
      }));
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  saveActiveDocument: async (force = false) => {
    const activePath = get().activeDocumentPath;
    const document = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!document) return;
    if (document.locator.kind === "untitled") {
      await get().saveActiveDocumentAs();
      return;
    }
    clearDocumentAutoSave(sessionSaveController);
    await saveNamedDocument(document.path, force, consoleClient, set, get);
  },
  saveActiveDocumentAs: async () => {
    const activePath = get().activeDocumentPath;
    const document = get().documents.find(
      (candidate) => candidate.path === activePath,
    );
    if (!document) return;
    clearDocumentAutoSave(sessionSaveController);
    const prepared = prepareDocumentForSave(
      document,
      get().settings,
      get().dialect,
      get().connection?.connectorKind !== "document" &&
        get().connection?.connectorKind !== "keyValue",
    );
    try {
      const saved = await consoleClient.saveDocumentAs({
        content: prepared.content,
        suggestedName:
          document.locator.kind === "untitled" ? document.name : document.name,
      });
      if (!saved) {
        set({ notice: "已取消保存" });
        return;
      }
      const open = toOpenDocument(saved);
      set((state) => ({
        documents: state.documents.map((candidate) =>
          candidate.path === activePath ? open : candidate,
        ),
        activeDocumentPath: open.path,
        sql: open.content,
        recentFiles: addRecentFile(state.recentFiles, saved),
        notice: `${open.name} · 已另存为`,
      }));
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
      throw normalized;
    }
  },
  saveAllDocuments: async () => {
    clearDocumentAutoSave(sessionSaveController);
    for (const document of get().documents.filter((candidate) => candidate.dirty)) {
      const prepared = prepareDocumentForSave(
        document,
        get().settings,
        get().dialect,
        get().connection?.connectorKind !== "document" &&
          get().connection?.connectorKind !== "keyValue",
      );
      try {
        const saved =
          prepared.locator.kind === "untitled"
            ? await consoleClient.saveDocumentAs({
                content: prepared.content,
                suggestedName: prepared.name,
              })
            : await saveOpenDocument(consoleClient, prepared);
        if (!saved) return;
        set((state) => ({
          documents: state.documents.map((candidate) =>
            candidate.path === document.path
              ? toOpenDocument(saved)
              : candidate,
          ),
          activeDocumentPath:
            state.activeDocumentPath === document.path
              ? saved.path
              : state.activeDocumentPath,
          recentFiles: addRecentFile(state.recentFiles, saved),
        }));
      } catch (error) {
        const normalized = normalizeDbmsError(error);
        set((state) => ({
          documents: state.documents.map((candidate) =>
            candidate.path === document.path
              ? { ...candidate, conflict: normalized.sqlState === "40001" }
              : candidate,
          ),
          notice: normalized.message,
        }));
        return;
      }
    }
    const active = get().documents.find(
      (document) => document.path === get().activeDocumentPath,
    );
    set({ sql: active?.content ?? "", notice: "全部 SQL 文件已保存" });
    await persistSession(consoleClient, get);
  },
  saveActiveDocumentOnFocusChange: async () => {
    const state = get();
    const document = state.documents.find(
      (candidate) => candidate.path === state.activeDocumentPath,
    );
    if (
      state.settings.files.autoSave !== "onFocusChange" ||
      !document?.dirty ||
      document.conflict ||
      document.locator.kind === "untitled"
    ) {
      return;
    }
    await state.saveActiveDocument();
  },
  formatActiveDocument: () => {
    const state = get();
    if (!state.activeDocumentPath) return;
    if (
      state.connection?.connectorKind === "document" ||
      state.connection?.connectorKind === "keyValue"
    ) {
      set({ notice: "当前命令语言不使用 SQL 格式化器" });
      return;
    }
    const dialect = getSqlDialect(state.dialect);
    state.setSql(formatSqlForDialect(state.sql, dialect));
    set({ notice: `格式化 SQL · ${dialect.label}` });
  },
  renameWorkspaceEntry: async (path, newName) => {
    const workspace = get().workspace;
    if (!workspace) return;
    try {
      const snapshot = await consoleClient.renameEntry(
        workspace.rootPath,
        path,
        newName,
      );
      const nextPath = renamedPath(path, newName);
      const documents = get().documents.map((document) => {
        if (
          document.locator.kind !== "workspace" ||
          document.locator.rootPath !== workspace.rootPath
        ) {
          return document;
        }
        const pathAfterRename = replacePathPrefix(
          document.locator.path,
          path,
          nextPath,
        );
        return {
          ...document,
          path: pathAfterRename,
          name: pathAfterRename.split("/").at(-1) ?? document.name,
          locator: {
            ...document.locator,
            path: pathAfterRename,
          },
        };
      });
      const activeIndex = get().documents.findIndex(
        (document) => document.path === get().activeDocumentPath,
      );
      const activeDocumentPath =
        activeIndex >= 0
          ? documents[activeIndex]?.path ?? null
          : get().activeDocumentPath;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath,
        notice: "项目条目已重命名",
      });
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  trashWorkspaceEntry: async (path) => {
    const workspace = get().workspace;
    if (!workspace) return;
    try {
      const snapshot = await consoleClient.trashEntry(workspace.rootPath, path);
      const documents = get().documents.filter(
        (document) =>
          document.locator.kind !== "workspace" ||
          document.locator.rootPath !== workspace.rootPath ||
          (document.locator.path !== path &&
            !document.locator.path.startsWith(`${path}/`)),
      );
      const activeDocumentPath = documents.some(
        (document) => document.path === get().activeDocumentPath,
      )
        ? get().activeDocumentPath
        : documents.at(-1)?.path ?? null;
      set({
        workspace: snapshot,
        documents,
        activeDocumentPath,
        sql:
          documents.find(
            (document) => document.path === activeDocumentPath,
          )?.content ?? "",
        notice: "项目条目已移入回收站",
      });
      await persistSession(consoleClient, get);
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ notice: normalized.message });
    }
  },
  } satisfies Partial<WorkbenchState>;
}
