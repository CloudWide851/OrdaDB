import { create } from "zustand";
import { initialSql, previewRows } from "../data/preview";
import type { QueryRow, QueryState, ResultTab } from "../types";

interface WorkbenchState {
  sql: string;
  schemaVisible: boolean;
  assistantVisible: boolean;
  activeResultTab: ResultTab;
  queryState: QueryState;
  rows: QueryRow[];
  errorMessage: string | null;
  durationMs: number | null;
  setSql: (sql: string) => void;
  toggleSchema: () => void;
  toggleAssistant: () => void;
  setActiveResultTab: (tab: ResultTab) => void;
  runPreviewQuery: () => Promise<void>;
}

const wait = (milliseconds: number) =>
  new Promise<void>((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });

export const useWorkbenchStore = create<WorkbenchState>((set, get) => ({
  sql: initialSql,
  schemaVisible: true,
  assistantVisible: true,
  activeResultTab: "data",
  queryState: "idle",
  rows: [],
  errorMessage: null,
  durationMs: null,
  setSql: (sql) => set({ sql }),
  toggleSchema: () => set((state) => ({ schemaVisible: !state.schemaVisible })),
  toggleAssistant: () =>
    set((state) => ({ assistantVisible: !state.assistantVisible })),
  setActiveResultTab: (activeResultTab) => set({ activeResultTab }),
  runPreviewQuery: async () => {
    const sql = get().sql.trim();

    if (!sql) {
      set({
        queryState: "error",
        errorMessage: "SQL 不能为空",
        rows: [],
        durationMs: null,
        activeResultTab: "logs",
      });
      return;
    }

    set({
      queryState: "running",
      errorMessage: null,
      durationMs: null,
      activeResultTab: "data",
    });

    await wait(420);

    if (/\berror\b/i.test(sql)) {
      set({
        queryState: "error",
        errorMessage: "预览执行被测试关键字 ERROR 中止",
        rows: [],
        durationMs: 18,
        activeResultTab: "logs",
      });
      return;
    }

    set({
      queryState: "success",
      rows: previewRows,
      errorMessage: null,
      durationMs: 36,
    });
  },
}));
