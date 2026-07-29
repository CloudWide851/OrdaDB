import { describe, expect, it, vi } from "vitest";
import {
  PreviewConsoleClient,
  type WorkspaceSessionV1,
} from "../lib/consoleClient";
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
    await connectPreview(store);
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
    expect(store.getState().resultBuffer).toMatchObject({
      rowCount: 5,
      totalRows: 5,
      droppedRows: 0,
    });
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

    await connectPreview(store);
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
      connectorId: "ordadb-native",
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
    await connectPreview(store);
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

  it("opens, creates, switches, saves, and closes explicit SQL project files", async () => {
    const consoleClient = new PreviewConsoleClient();
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store.getState().initialize();
    expect(store.getState()).toMatchObject({
      workspace: null,
      documents: [],
      activeDocumentPath: null,
      sql: "",
    });

    await store.getState().openWorkspace();
    expect(store.getState().documents).toEqual([]);
    await store.getState().openDocument("queries/customers.sql");
    await store.getState().openDocument("scratch.sql");
    expect(store.getState().activeDocumentPath).toBe("scratch.sql");
    store.getState().activateDocument("queries/customers.sql");
    expect(store.getState().sql).toBe("select * from customers;\n");

    store.getState().setSql("select id from customers;\n");
    expect(store.getState().documents[0]).toMatchObject({ dirty: true });
    await store.getState().saveActiveDocument();
    expect(store.getState().documents[0]).toMatchObject({
      content: "select id from customers;\n",
      dirty: false,
      conflict: false,
    });

    await store.getState().createDocument();
    expect(store.getState().activeDocumentPath).toBe("query.sql");
    store.getState().setSql("select 42;\n");
    await store.getState().saveAllDocuments();
    expect(store.getState().documents.at(-1)).toMatchObject({
      content: "select 42;\n",
      dirty: false,
    });
    await store.getState().closeDocument("query.sql");
    expect(store.getState().activeDocumentPath).toBe("scratch.sql");
  });

  it("marks external conflicts and supports reload or one-time overwrite", async () => {
    const consoleClient = new PreviewConsoleClient();
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);
    await store.getState().openWorkspace();
    await store.getState().openDocument("scratch.sql");
    const opened = store.getState().documents[0];
    store.getState().setSql("select 'local';\n");

    await consoleClient.saveDocument(
      "Preview fixture",
      {
        ...opened,
        content: "select 'external';\n",
      },
      true,
    );
    await expect(store.getState().saveActiveDocument()).rejects.toMatchObject({
      sqlState: "40001",
    });
    expect(store.getState().documents[0]).toMatchObject({
      content: "select 'local';\n",
      dirty: true,
      conflict: true,
    });

    await store.getState().reloadActiveDocument();
    expect(store.getState().documents[0]).toMatchObject({
      content: "select 'external';\n",
      dirty: false,
      conflict: false,
    });
    store.getState().setSql("select 'overwrite';\n");
    await store.getState().saveActiveDocument(true);
    expect(store.getState().documents[0]).toMatchObject({
      content: "select 'overwrite';\n",
      dirty: false,
      conflict: false,
    });
  });

  it("keeps recovery explicit and reports a changed base revision", async () => {
    const consoleClient = new PreviewConsoleClient();
    const opened = await consoleClient.openDocument(
      "Preview fixture",
      "scratch.sql",
    );
    const draft: WorkspaceSessionV1 = {
      formatVersion: 1,
      rootPath: "Preview fixture",
      activePath: "scratch.sql",
      openDocuments: [
        {
          path: "scratch.sql",
          content: "select 'draft';\n",
          baseRevision: opened.revision,
        },
      ],
    };
    await consoleClient.saveSession(draft);
    await consoleClient.saveDocument(
      "Preview fixture",
      {
        ...opened,
        savedContent: opened.content,
        dirty: false,
        conflict: false,
        content: "select 'external';\n",
      },
      true,
    );

    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);
    await store.getState().initialize();
    expect(store.getState()).toMatchObject({
      workspace: null,
      documents: [],
      recovery: draft,
    });
    await store.getState().restoreRecovery();
    expect(store.getState().documents[0]).toMatchObject({
      content: "select 'draft';\n",
      savedContent: "select 'external';\n",
      dirty: true,
      conflict: true,
    });
  });

  it("keeps debounced draft persistence independent between stores", async () => {
    vi.useFakeTimers();
    try {
      const firstClient = new PreviewConsoleClient();
      const secondClient = new PreviewConsoleClient();
      const firstSave = vi.spyOn(firstClient, "saveSession");
      const secondSave = vi.spyOn(secondClient, "saveSession");
      const firstStore = createWorkbenchStore(
        new PreviewDbmsClient(),
        firstClient,
      );
      const secondStore = createWorkbenchStore(
        new PreviewDbmsClient(),
        secondClient,
      );

      firstStore.getState().setSql("select 'first';");
      secondStore.getState().setSql("select 'second';");
      await vi.advanceTimersByTimeAsync(500);

      expect(firstSave).toHaveBeenCalledTimes(1);
      expect(secondSave).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});

async function connectPreview(
  store: ReturnType<typeof createWorkbenchStore>,
) {
  await store.getState().connectDataSource({
    connectorId: "ordadb-native",
    dialect: "postgresql",
    endpoint: "preview",
    database: "ordadb_preview",
    credentialId: "preview-test",
    username: "dba",
    password: "disposable-secret",
  });
}
