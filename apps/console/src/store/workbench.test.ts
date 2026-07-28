import { describe, expect, it } from "vitest";
import {
  PreviewDbmsClient,
  type DbmsClient,
  type DbmsQueryOperation,
} from "../lib/dbmsClient";
import { createWorkbenchStore } from "./workbench";

describe("workbench store", () => {
  it("projects ordered query events into dynamic result state", async () => {
    const store = createWorkbenchStore(new PreviewDbmsClient());

    await store.getState().initialize();
    await store.getState().runQuery({ sql: "select 1" });

    expect(store.getState()).toMatchObject({
      queryState: "success",
      rowsProcessed: 5,
      durationMs: 36,
      activeRequestId: null,
      error: null,
    });
    expect(store.getState().columns.map((column) => column.name)).toEqual([
      "id",
      "title",
      "category",
      "score",
      "updated_at",
    ]);
    expect(store.getState().rows).toHaveLength(5);
    expect(store.getState().logs).toContain(
      "Preview fixture · 不连接真实数据库",
    );
  });

  it("rejects a query stream that ends before a terminal event", async () => {
    class IncompleteClient extends PreviewDbmsClient {
      override execute: DbmsClient["execute"] = async (): Promise<DbmsQueryOperation> => {
        return {
          requestId: "incomplete",
          events: (async function* () {
            yield {
              kind: "schema" as const,
              columns: [{ name: "value", dataType: "text" }],
            };
          })(),
        };
      }
    }
    const store = createWorkbenchStore(new IncompleteClient());

    await store.getState().runQuery({ sql: "select 1" });

    expect(store.getState().queryState).toBe("error");
    expect(store.getState().error).toMatchObject({
      sqlState: "XX000",
      message: expect.stringContaining("Complete"),
    });
  });

  it("keeps passwords outside Zustand state while retaining an opaque credential ID", async () => {
    const store = createWorkbenchStore(new PreviewDbmsClient());

    await store.getState().connectDataSource({
      connectorId: "ordadb-postgresql",
      dialect: "postgresql",
      endpoint: "preview",
      database: "ordadb_preview",
      credentialId: "preview-credential",
      username: "dba",
      password: "disposable-secret",
    });

    expect(store.getState().activeCredentialId).toBe("preview-credential");
    expect(store.getState()).not.toHaveProperty("password");
    expect(JSON.stringify(store.getState())).not.toContain("disposable-secret");
  });

  it("tracks administration operations without exposing native paths", async () => {
    const store = createWorkbenchStore(new PreviewDbmsClient());

    await store.getState().initialize();
    await store.getState().startAdministrationOperation({
      kind: "export",
      path: "documents.csv",
      schema: "public",
      table: "documents",
      format: "csv",
    });

    expect(store.getState().operations[0]).toMatchObject({
      kind: "export",
      state: "queued",
      path: "documents.csv",
    });
    await store.getState().refreshAdministration();
    expect(store.getState().operations[0]).toMatchObject({
      kind: "export",
      state: "succeeded",
    });
    expect(store.getState().serviceStatus).toMatchObject({
      name: "OrdaDB Preview",
      dataDir: "Preview fixture",
    });
  });
});
