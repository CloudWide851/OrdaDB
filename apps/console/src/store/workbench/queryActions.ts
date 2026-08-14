import { normalizeDbmsError, type DbmsCommand } from "../../lib/dbmsClient";
import { appendResultRows, emptyResultBuffer } from "../../lib/resultBuffer";
import type { WorkbenchActionContext } from "./context";
import {
  appendStructuredValues,
  buildDbmsCommand,
  localError,
  queryTimeoutError,
  requiresDangerousWriteConfirmation,
  resultBufferLimits,
  resultItemCount,
  runTransaction,
  setQueryError,
  withTimeout,
} from "./databaseSupport";
import type { WorkbenchState } from "./types";

export function createQueryActions({
  dbms,
  get,
  set,
}: WorkbenchActionContext) {
  return {  runQuery: async (options = {}) => {
    const input = (options.sql ?? get().sql).trim();
    const connection = get().connection;
    if (!input) {
      const error = localError("42601", "命令不能为空");
      setQueryError(set, error);
      return;
    }
    if (!connection) {
      set({
        dataSourceOpen: true,
        notice: "请先连接数据源",
      });
      return;
    }
    if (
      get().settings.connections.confirmDangerousWrites &&
      connection.connectorKind === "sql" &&
      requiresDangerousWriteConfirmation(input) &&
      (typeof window === "undefined" ||
        !window.confirm("该语句可能修改数据库。确认继续执行吗？"))
    ) {
      set({ notice: "已取消可能修改数据库的语句" });
      return;
    }
    const queryTimeoutMs = get().settings.results.queryTimeoutMs;
    const queryDeadline = Date.now() + queryTimeoutMs;
    const resultLimits = resultBufferLimits(get().settings);
    let command: DbmsCommand;
    try {
      command = buildDbmsCommand(connection, input);
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
      return;
    }
    set({
      queryState: "running",
      columns: [],
      resultBuffer: emptyResultBuffer(),
      documentResults: [],
      keyValueResults: [],
      structuredResultBytes: 0,
      droppedStructuredItems: 0,
      logs: [],
      error: null,
      errorMessage: null,
      durationMs: null,
      rowsProcessed: 0,
      activeRequestId: null,
      activeResultTab: options.resultTab ?? "data",
      notice:
        connection.mode === "preview" ? "正在运行 Preview 命令" : "正在运行命令",
    });
    try {
      const operation = await withTimeout(
        dbms.execute(connection.connectionId, command),
        queryTimeoutMs,
        () => queryTimeoutError(queryTimeoutMs),
      );
      set({ activeRequestId: operation.requestId });
      let terminal = false;
      const iterator = operation.events[Symbol.asyncIterator]();
      while (true) {
        const remainingMs = Math.max(1, queryDeadline - Date.now());
        let next: IteratorResult<
          Awaited<ReturnType<typeof iterator.next>>["value"]
        >;
        try {
          next = await withTimeout(iterator.next(), remainingMs, () =>
            queryTimeoutError(queryTimeoutMs),
          );
        } catch (error) {
          const normalized = normalizeDbmsError(error);
          if (normalized.sqlState === "57014") {
            void dbms.cancel(operation.requestId).catch(() => undefined);
            void Promise.resolve(iterator.return?.()).catch(() => undefined);
          }
          throw normalized;
        }
        if (next.done) break;
        const event = next.value;
        switch (event.kind) {
          case "schema":
            set({ columns: event.columns });
            break;
          case "batch":
            set((state) => ({
              resultBuffer: appendResultRows(
                state.resultBuffer,
                event.rows,
                resultLimits,
              ),
            }));
            break;
          case "documents":
            set((state) => {
              const appended = appendStructuredValues(
                state.documentResults,
                event.documents,
                state.structuredResultBytes,
                state.droppedStructuredItems,
                state.settings,
              );
              return {
                documentResults: appended.items,
                structuredResultBytes: appended.bytes,
                droppedStructuredItems: appended.droppedItems,
              };
            });
            break;
          case "keyValues":
            set((state) => {
              const appended = appendStructuredValues(
                state.keyValueResults,
                event.entries,
                state.structuredResultBytes,
                state.droppedStructuredItems,
                state.settings,
              );
              return {
                keyValueResults: appended.items,
                structuredResultBytes: appended.bytes,
                droppedStructuredItems: appended.droppedItems,
              };
            });
            break;
          case "progress":
            set({ rowsProcessed: event.rowsProcessed });
            break;
          case "notice":
            set((state) => ({ logs: [...state.logs, event.message] }));
            break;
          case "complete":
            terminal = true;
            set((state) => ({
              queryState: "success",
              durationMs: event.durationMs,
              activeRequestId: null,
              logs: [...state.logs, event.commandTag],
              notice: `${event.commandTag} · ${resultItemCount(state)} 项`,
            }));
            break;
          case "error":
            terminal = true;
            setQueryError(set, event.error);
            break;
        }
      }
      if (!terminal) {
        setQueryError(
          set,
          localError("XX000", "查询事件流在 Complete 之前结束"),
        );
      }
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
    }
  },

  runExplain: async () => {
    const connection = get().connection;
    if (connection && !connection.capabilities.explain) {
      setQueryError(set, localError("0A000", "当前数据源不支持执行计划"));
      return;
    }
    const sql = get().sql.trim();
    if (!sql) {
      setQueryError(set, localError("42601", "SQL 不能为空"));
      return;
    }
    await get().runQuery({ sql: `EXPLAIN ${sql}`, resultTab: "plan" });
  },

  cancelQuery: async () => {
    const requestId = get().activeRequestId;
    if (!requestId) return;
    try {
      await dbms.cancel(requestId);
      set({ notice: "已发送取消请求" });
    } catch (error) {
      setQueryError(set, normalizeDbmsError(error));
    }
  },

  beginTransaction: async () => {
    await runTransaction("begin", dbms, set, get);
  },
  commitTransaction: async () => {
    await runTransaction("commit", dbms, set, get);
  },
  rollbackTransaction: async () => {
    await runTransaction("rollback", dbms, set, get);
  },

  checkpoint: async () => {
    const connection = get().connection;
    if (!connection) {
      set({ dataSourceOpen: true, notice: "请先连接数据源" });
      return;
    }
    try {
      const storage = await dbms.checkpoint(connection.connectionId);
      set((state) => ({
        monitor: state.monitor
          ? {
              ...state.monitor,
              storage,
              wal: storage,
            }
          : null,
        notice: `检查点完成 · LSN ${storage.durableLsn ?? "—"}`,
      }));
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({ connectionError: normalized, notice: normalized.message });
    }
  },
  } satisfies Partial<WorkbenchState>;
}
