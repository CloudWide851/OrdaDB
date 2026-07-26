import { create } from "zustand";
import { getSqlDialect } from "../data/dialects";
import { initialSql, previewRows } from "../data/preview";
import type {
  InspectorTab,
  QueryRow,
  QueryState,
  ResultTab,
  SqlDialect,
} from "../types";

interface WorkbenchState {
  sql: string;
  dialect: SqlDialect;
  schemaVisible: boolean;
  inspectorVisible: boolean;
  activeResultTab: ResultTab;
  activeInspectorTab: InspectorTab;
  selectedObject: string;
  commandPaletteOpen: boolean;
  notice: string;
  queryState: QueryState;
  rows: QueryRow[];
  errorMessage: string | null;
  durationMs: number | null;
  setSql: (sql: string) => void;
  setDialect: (dialect: SqlDialect) => void;
  toggleSchema: () => void;
  toggleInspector: () => void;
  setActiveResultTab: (tab: ResultTab) => void;
  setActiveInspectorTab: (tab: InspectorTab) => void;
  setSelectedObject: (objectName: string) => void;
  setCommandPaletteOpen: (open: boolean) => void;
  setNotice: (notice: string) => void;
  runPreviewQuery: () => Promise<void>;
}

const wait = (milliseconds: number) =>
  new Promise<void>((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });

export const useWorkbenchStore = create<WorkbenchState>((set, get) => ({
  sql: initialSql,
  dialect: "postgresql",
  schemaVisible: true,
  inspectorVisible: true,
  activeResultTab: "data",
  activeInspectorTab: "properties",
  selectedObject: "documents",
  commandPaletteOpen: false,
  notice: "准备就绪",
  queryState: "idle",
  rows: [],
  errorMessage: null,
  durationMs: null,
  setSql: (sql) => set({ sql }),
  setDialect: (dialect) =>
    set({
      dialect,
      notice: `SQL 方言已切换 · ${getSqlDialect(dialect).label} · 预览`,
    }),
  toggleSchema: () => set((state) => ({ schemaVisible: !state.schemaVisible })),
  toggleInspector: () =>
    set((state) => ({ inspectorVisible: !state.inspectorVisible })),
  setActiveResultTab: (activeResultTab) => set({ activeResultTab }),
  setActiveInspectorTab: (activeInspectorTab) => set({ activeInspectorTab }),
  setSelectedObject: (selectedObject) => set({ selectedObject }),
  setCommandPaletteOpen: (commandPaletteOpen) => set({ commandPaletteOpen }),
  setNotice: (notice) => set({ notice }),
  runPreviewQuery: async () => {
    const sql = get().sql.trim();

    if (!sql) {
      set({
        queryState: "error",
        errorMessage: "SQL 不能为空",
        rows: [],
        durationMs: null,
        activeResultTab: "logs",
        notice: "查询失败",
      });
      return;
    }

    set({
      queryState: "running",
      errorMessage: null,
      durationMs: null,
      activeResultTab: "data",
      notice: "正在运行预览查询",
    });

    await wait(420);

    if (/\berror\b/i.test(sql)) {
      set({
        queryState: "error",
        errorMessage: "预览执行被测试关键字 ERROR 中止",
        rows: [],
        durationMs: 18,
        activeResultTab: "logs",
        notice: "预览查询已中止",
      });
      return;
    }

    set({
      queryState: "success",
      rows: previewRows,
      errorMessage: null,
      durationMs: 36,
      notice: "预览查询完成 · 5 行",
    });
  },
}));
