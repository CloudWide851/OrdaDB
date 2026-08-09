import { describe, expect, it, vi } from "vitest";
import {
  PreviewConsoleClient,
  type SqlDocument,
  type WorkspaceSessionV1,
} from "../lib/consoleClient";
import {
  PreviewDbmsClient,
  type ConnectionProbe,
  type DbmsClient,
  type DbmsQueryOperation,
} from "../lib/dbmsClient";
import { createWorkbenchStore, type DataSourceValues } from "./workbench";

describe("workbench store", () => {
  it("applies committed appearance settings to the runtime root", async () => {
    const store = createWorkbenchStore(
      new PreviewDbmsClient(),
      new PreviewConsoleClient(),
    );
    const settings = {
      ...store.getState().settings,
      appearance: {
        ...store.getState().settings.appearance,
        theme: "dark" as const,
        zoomPercent: 90,
        density: "comfortable" as const,
      },
    };

    await store.getState().saveSettings(settings);

    expect(document.documentElement.dataset).toMatchObject({
      theme: "dark",
      density: "comfortable",
    });
    expect(
      document.documentElement.style.getPropertyValue("--ui-zoom"),
    ).toBe("0.9");
  });

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

  it("routes MongoDB JSON and Redis arguments without SQL parsing or monitor calls", async () => {
    const mongodbClient = new PreviewDbmsClient();
    const mongodbMonitor = vi.spyOn(mongodbClient, "monitor");
    const mongodbExecute = vi.spyOn(mongodbClient, "execute");
    const mongodb = createWorkbenchStore(mongodbClient);
    await mongodb.getState().connectDataSource({
      connectorId: "mongodb",
      connectorKind: "document",
      commandLanguage: "mongodb-json",
      endpoint: "preview",
      database: "admin",
      credentialId: "preview-mongodb",
      username: "dba",
      tlsMode: "prefer",
    });
    await mongodb
      .getState()
      .runQuery({ sql: '{"operation":"find","collection":"items"}' });

    expect(mongodbMonitor).not.toHaveBeenCalled();
    expect(mongodbExecute).toHaveBeenCalledWith(
      "preview-connection",
      expect.objectContaining({
        kind: "document",
        languageId: "mongodb-json",
        document: { operation: "find", collection: "items" },
      }),
    );
    expect(mongodb.getState()).toMatchObject({
      queryState: "success",
      documentResults: [expect.any(Object)],
      resultBuffer: { rowCount: 0 },
    });

    const redisClient = new PreviewDbmsClient();
    const redisExecute = vi.spyOn(redisClient, "execute");
    const redis = createWorkbenchStore(redisClient);
    await redis.getState().connectDataSource({
      connectorId: "redis",
      connectorKind: "keyValue",
      commandLanguage: "redis-resp3",
      endpoint: "preview",
      database: "0",
      credentialId: "preview-redis",
      username: "default",
      tlsMode: "disable",
    });
    await redis.getState().runQuery({ sql: 'SET "customer name" "Ada Lovelace"' });

    expect(redisExecute).toHaveBeenCalledWith("preview-connection", {
      kind: "arguments",
      languageId: "redis-resp3",
      arguments: ["SET", "customer name", "Ada Lovelace"],
    });
    expect(redis.getState()).toMatchObject({
      queryState: "success",
      keyValueResults: [expect.any(Object)],
      resultBuffer: { rowCount: 0 },
    });
  });

  it("rejects invalid MongoDB JSON before invoking the connector", async () => {
    const client = new PreviewDbmsClient();
    const execute = vi.spyOn(client, "execute");
    const store = createWorkbenchStore(client);
    await store.getState().connectDataSource({
      connectorId: "mongodb",
      connectorKind: "document",
      commandLanguage: "mongodb-json",
      endpoint: "preview",
      database: "admin",
      credentialId: "preview-mongodb-invalid",
      username: "dba",
      tlsMode: "prefer",
    });

    await store.getState().runQuery({ sql: "{ invalid" });

    expect(execute).not.toHaveBeenCalled();
    expect(store.getState().error).toMatchObject({ sqlState: "22023" });
  });

  it("applies configured result paging and resident limits", async () => {
    class LargeResultClient extends PreviewDbmsClient {
      override execute: DbmsClient["execute"] = async () => ({
        requestId: "large-result",
        events: (async function* () {
          yield {
            kind: "schema" as const,
            columns: [{ name: "value", dataType: "text" }],
          };
          yield {
            kind: "batch" as const,
            rows: Array.from({ length: 105 }, (_, index) => [
              index.toString(),
            ]),
          };
          yield {
            kind: "complete" as const,
            commandTag: "SELECT 105",
            durationMs: 1,
          };
        })(),
      });
    }
    const store = createWorkbenchStore(new LargeResultClient());
    store.setState((state) => ({
      settings: {
        ...state.settings,
        results: {
          ...state.settings.results,
          pageSize: 50,
          residentRowLimit: 100,
        },
      },
    }));

    await connectPreview(store);
    await store.getState().runQuery({ sql: "select 1" });

    expect(store.getState().resultBuffer).toMatchObject({
      rowCount: 100,
      totalRows: 105,
      droppedRows: 5,
    });
    expect(store.getState().resultBuffer.pages).toHaveLength(2);
  });

  it("cancels a query at the configured deadline", async () => {
    vi.useFakeTimers();
    try {
      class HangingQueryClient extends PreviewDbmsClient {
        override execute: DbmsClient["execute"] = async () => ({
          requestId: "timed-query",
          events: {
            [Symbol.asyncIterator]() {
              return {
                next: () => new Promise<IteratorResult<never>>(() => undefined),
                return: async () => ({ done: true, value: undefined }),
              };
            },
          },
        });
      }
      const dbms = new HangingQueryClient();
      const cancel = vi.spyOn(dbms, "cancel");
      const store = createWorkbenchStore(dbms);
      store.setState((state) => ({
        settings: {
          ...state.settings,
          results: { ...state.settings.results, queryTimeoutMs: 1_000 },
        },
      }));
      await connectPreview(store);

      const running = store.getState().runQuery({ sql: "select 1" });
      await vi.advanceTimersByTimeAsync(1_000);
      await running;

      expect(cancel).toHaveBeenCalledWith("timed-query");
      expect(store.getState().error).toMatchObject({ sqlState: "57014" });
    } finally {
      vi.useRealTimers();
    }
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

  it("prompts natively without sending passwords through Zustand", async () => {
    const dbms = new PreviewDbmsClient();
    const promptCredential = vi.spyOn(dbms, "promptCredential");
    const store = createWorkbenchStore(dbms);

    await store.getState().connectDataSource({
      connectorId: "ordadb-native",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "preview",
      database: "ordadb_preview",
      credentialId: "preview-credential",
      username: "dba",
      tlsMode: "disable",
    });

    expect(store.getState().activeCredentialId).toBe("preview-credential");
    expect(store.getState()).not.toHaveProperty("password");
    expect(promptCredential).toHaveBeenCalledWith({
      credentialId: "preview-credential",
      connectorId: "ordadb-native",
      suggestedUsername: "dba",
    });
    expect(JSON.stringify(promptCredential.mock.calls)).not.toMatch(/password/i);
  });

  it("does not persist a native credential or profile before consuming a bootstrap ticket", async () => {
    const dbms = new PreviewDbmsClient();
    const consoleClient = new PreviewConsoleClient();
    const promptCredential = vi.spyOn(dbms, "promptCredential");
    const connect = vi.spyOn(dbms, "connect");
    const bootstrapAdmin = vi.spyOn(dbms, "bootstrapAdmin").mockResolvedValue({
      success: true,
      user: "ordadb_admin",
      error: null,
    });
    vi.spyOn(dbms, "probe")
      .mockResolvedValueOnce(bootstrapRequiredProbe())
      .mockResolvedValueOnce(readyProbe());
    const saveProfile = vi.spyOn(consoleClient, "saveConnectionProfile");
    const store = createWorkbenchStore(dbms, consoleClient);
    const values = nativeDataSourceValues();

    await expect(store.getState().connectDataSource(values)).rejects.toMatchObject({
      sqlState: "55000",
    });
    expect(promptCredential).not.toHaveBeenCalled();
    expect(saveProfile).not.toHaveBeenCalled();
    expect(connect).not.toHaveBeenCalled();
    expect(store.getState().connectionProbe?.bootstrapTicket).toEqual({
      ticket: "local-bootstrap-ticket",
      expiresInMs: 120_000,
    });

    await store.getState().bootstrapAdministrator(values);

    expect(bootstrapAdmin).toHaveBeenCalledWith({
      ticket: "local-bootstrap-ticket",
      connection: {
        connectorId: "ordadb-native",
        connectorKind: "sql",
        commandLanguage: "postgresql-sql",
        dialect: "postgresql",
        endpoint: "127.0.0.1:54329",
        adminEndpoint: "http://127.0.0.1:9080",
        database: "ordadb",
        tlsMode: "disable",
        credentialId: "ordadb-local",
      },
      suggestedUsername: "ordadb_admin",
    });
    expect(JSON.stringify(bootstrapAdmin.mock.calls)).not.toMatch(/password/i);
    expect(promptCredential).not.toHaveBeenCalled();
    expect(connect).toHaveBeenCalledTimes(1);
    expect(saveProfile).toHaveBeenCalledTimes(1);
    expect(store.getState()).toMatchObject({
      connectionState: "connected",
      activeCredentialId: "ordadb-local",
    });
  });

  it("keeps external PostgreSQL out of the native bootstrap flow", async () => {
    const dbms = new PreviewDbmsClient();
    const probe = vi.spyOn(dbms, "probe").mockResolvedValue(readyProbe());
    const bootstrapAdmin = vi.spyOn(dbms, "bootstrapAdmin");
    const promptCredential = vi.spyOn(dbms, "promptCredential");
    const store = createWorkbenchStore(dbms, new PreviewConsoleClient());
    const values: DataSourceValues = {
      connectorId: "postgresql",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "db.example.test:5432",
      database: "app",
      credentialId: "postgresql-app",
      username: "app_user",
      tlsMode: "verifyFull",
    };

    await store.getState().connectDataSource(values);

    expect(probe).toHaveBeenCalledTimes(1);
    expect(probe).toHaveBeenCalledWith({
      connectorId: "postgresql",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "db.example.test:5432",
      adminEndpoint: undefined,
      database: "app",
      tlsMode: "verifyFull",
      credentialId: "postgresql-app",
    });
    expect(promptCredential).toHaveBeenCalledWith({
      credentialId: "postgresql-app",
      connectorId: "postgresql",
      suggestedUsername: "app_user",
    });
    expect(JSON.stringify(promptCredential.mock.calls)).not.toMatch(/password/i);
    expect(bootstrapAdmin).not.toHaveBeenCalled();
    expect(store.getState().connection?.connectorId).toBe("postgresql");
  });

  it("times out connection I/O without timing the native credential prompt", async () => {
    vi.useFakeTimers();
    try {
      class HangingProbeClient extends PreviewDbmsClient {
        override probe = async () =>
          new Promise<ConnectionProbe>(() => undefined);
      }
      const dbms = new HangingProbeClient();
      const promptCredential = vi.spyOn(dbms, "promptCredential");
      const store = createWorkbenchStore(dbms);
      store.setState((state) => ({
        settings: {
          ...state.settings,
          connections: { ...state.settings.connections, timeoutMs: 1_000 },
        },
      }));

      const connecting = store
        .getState()
        .connectDataSource({
          connectorId: "postgresql",
          connectorKind: "sql",
          commandLanguage: "postgresql-sql",
          dialect: "postgresql",
          endpoint: "db.example.test:5432",
          database: "app",
          credentialId: "postgresql-timeout",
          username: "app_user",
          tlsMode: "verifyFull",
        })
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(1_000);
      const error = await connecting;

      expect(promptCredential).toHaveBeenCalledTimes(1);
      expect(error).toMatchObject({ sqlState: "08001" });
      expect(store.getState().connectionState).toBe("error");
    } finally {
      vi.useRealTimers();
    }
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

  it("creates multiple untitled SQL documents without opening a project and saves on first request", async () => {
    const consoleClient = new PreviewConsoleClient();
    const saveAs = vi.spyOn(consoleClient, "saveDocumentAs");
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store.getState().createDocument();
    await store.getState().createDocument();
    expect(store.getState()).toMatchObject({
      workspace: null,
      activeDocumentPath: "untitled:untitled-2",
    });
    expect(store.getState().documents.map((document) => document.name)).toEqual([
      "未命名-1.sql",
      "未命名-2.sql",
    ]);

    store.getState().setSql("select 2;\n");
    await store.getState().saveActiveDocument();

    expect(saveAs).toHaveBeenCalledWith({
      content: "select 2;\n",
      suggestedName: "未命名-2.sql",
    });
    expect(store.getState().activeDocumentPath).toBe("未命名-2.sql");
    expect(store.getState().documents.at(-1)).toMatchObject({
      locator: { kind: "workspace", rootPath: "Preview fixture" },
      dirty: false,
    });
  });

  it("formats SQL on save when the setting is enabled", async () => {
    const consoleClient = new PreviewConsoleClient();
    const saveAs = vi.spyOn(consoleClient, "saveDocumentAs");
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);
    store.setState((state) => ({
      settings: {
        ...state.settings,
        editor: { ...state.settings.editor, formatOnSave: true },
      },
    }));

    await store.getState().createDocument();
    store.getState().setSql("select id from customers;");
    await store.getState().saveActiveDocument();

    expect(saveAs).toHaveBeenCalledWith({
      content: "SELECT id FROM customers;",
      suggestedName: "未命名-1.sql",
    });
    expect(store.getState().sql).toBe("SELECT id FROM customers;");
  });

  it("keeps an untitled document when Save As is cancelled", async () => {
    const consoleClient = new PreviewConsoleClient();
    vi.spyOn(consoleClient, "saveDocumentAs").mockResolvedValue(null);
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store.getState().createDocument();
    store.getState().setSql("select 'draft';\n");
    await store.getState().saveActiveDocument();

    expect(store.getState().documents[0]).toMatchObject({
      locator: { kind: "untitled", id: "untitled-1" },
      content: "select 'draft';\n",
      dirty: true,
    });
    expect(store.getState().notice).toBe("已取消保存");
  });

  it("deduplicates normalized external paths and handles external conflicts", async () => {
    const consoleClient = new PreviewConsoleClient();
    let diskDocument = externalDocument(
      "C:\\SQL\\report.sql",
      "select 'disk';\n",
      1,
    );
    const openExternal = vi
      .spyOn(consoleClient, "openExternalDocument")
      .mockImplementation(async () => diskDocument);
    const saveExternal = vi
      .spyOn(consoleClient, "saveExternalDocument")
      .mockImplementation(async (document, force = false) => {
        if (
          !force &&
          document.revision?.sha256 !== diskDocument.revision.sha256
        ) {
          throw dbmsTestError("40001", "SQL 文件已在外部修改");
        }
        diskDocument = externalDocument(
          diskDocument.locator.kind === "external"
            ? diskDocument.locator.path
            : diskDocument.path,
          document.content,
          diskDocument.revision.modifiedAtMs + 1,
        );
        return diskDocument;
      });
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store
      .getState()
      .openExternalFiles([
        "C:\\SQL\\REPORT.SQL",
        "c:\\sql\\report.sql",
        "C:\\SQL\\notes.txt",
      ]);
    expect(openExternal).toHaveBeenCalledTimes(2);
    expect(store.getState().documents).toHaveLength(1);
    expect(store.getState().recentFiles).toHaveLength(1);

    store.getState().setSql("select 'local';\n");
    diskDocument = externalDocument(
      "C:\\SQL\\report.sql",
      "select 'external';\n",
      2,
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
    expect(saveExternal).toHaveBeenLastCalledWith(
      expect.objectContaining({ content: "select 'overwrite';\n" }),
      true,
    );
    expect(store.getState().documents[0].conflict).toBe(false);
  });

  it("caps recent SQL files at fifty entries", async () => {
    const consoleClient = new PreviewConsoleClient();
    vi.spyOn(consoleClient, "openExternalDocument").mockImplementation(
      async (path) => externalDocument(path, "select 1;\n", 1),
    );
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);
    const paths = Array.from(
      { length: 55 },
      (_, index) => `C:\\SQL\\query-${String(index).padStart(2, "0")}.sql`,
    );

    await store.getState().openExternalFiles(paths);

    expect(store.getState().recentFiles).toHaveLength(50);
    expect(store.getState().recentFiles[0].name).toBe("query-54.sql");
    expect(store.getState().recentFiles.at(-1)?.name).toBe("query-05.sql");
  });

  it("restores untitled drafts without inventing a disk location", async () => {
    const consoleClient = new PreviewConsoleClient();
    await consoleClient.saveSession({
      formatVersion: 1,
      rootPath: null,
      activePath: "untitled:recovered-1",
      openDocuments: [
        {
          locator: { kind: "untitled", id: "recovered-1" },
          path: "untitled:recovered-1",
          name: "未命名-7.sql",
          content: "select 'recovered';\n",
          baseRevision: null,
        },
      ],
    });
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store.getState().initialize();
    await store.getState().restoreRecovery();

    expect(store.getState().documents[0]).toMatchObject({
      locator: { kind: "untitled", id: "recovered-1" },
      path: "untitled:recovered-1",
      name: "未命名-7.sql",
      content: "select 'recovered';\n",
      dirty: true,
    });
  });

  it("clears saved recovery when the recovery policy is never", async () => {
    const consoleClient = new PreviewConsoleClient();
    await consoleClient.saveSession({
      formatVersion: 1,
      rootPath: null,
      activePath: "untitled:discarded",
      openDocuments: [
        {
          locator: { kind: "untitled", id: "discarded" },
          path: "untitled:discarded",
          name: "未命名-1.sql",
          content: "select 1;",
          baseRevision: null,
        },
      ],
    });
    const defaults = (await consoleClient.bootstrap()).settings;
    await consoleClient.saveSettings({
      ...defaults,
      files: { ...defaults.files, recoveryPolicy: "never" },
    });
    const store = createWorkbenchStore(new PreviewDbmsClient(), consoleClient);

    await store.getState().initialize();

    expect(store.getState().recovery).toBeNull();
    expect((await consoleClient.bootstrap()).recovery).toBeNull();
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
    expect(store.getState().activeDocumentPath).toBe("untitled:untitled-1");
    expect(store.getState().documents.at(-1)).toMatchObject({
      locator: { kind: "untitled", id: "untitled-1" },
      name: "未命名-1.sql",
      dirty: true,
    });
    store.getState().setSql("select 42;\n");
    await store.getState().saveAllDocuments();
    expect(store.getState().documents.at(-1)).toMatchObject({
      path: "未命名-1.sql",
      content: "select 42;\n",
      dirty: false,
    });
    await store.getState().closeDocument("未命名-1.sql");
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

  it("auto-saves a named dirty document after the configured delay", async () => {
    vi.useFakeTimers();
    try {
      const consoleClient = new PreviewConsoleClient();
      const saveDocument = vi.spyOn(consoleClient, "saveDocument");
      const store = createWorkbenchStore(
        new PreviewDbmsClient(),
        consoleClient,
      );
      await store.getState().openWorkspace();
      await store.getState().openDocument("scratch.sql");
      store.setState((state) => ({
        settings: {
          ...state.settings,
          files: {
            ...state.settings.files,
            autoSave: "afterDelay",
            autoSaveDelayMs: 250,
          },
        },
      }));

      store.getState().setSql("select 'auto';\n");
      await vi.advanceTimersByTimeAsync(250);
      await Promise.resolve();

      expect(saveDocument).toHaveBeenCalledTimes(1);
      await vi.waitFor(() => {
        expect(store.getState().documents[0]).toMatchObject({
          content: "select 'auto';\n",
          dirty: false,
        });
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("requires confirmation before executing a potentially mutating statement", async () => {
    const dbms = new PreviewDbmsClient();
    const execute = vi.spyOn(dbms, "execute");
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    const store = createWorkbenchStore(dbms);
    await connectPreview(store);

    await store.getState().runQuery({ sql: "/* mutation */ UPDATE items SET value = 1" });

    expect(confirm).toHaveBeenCalledTimes(1);
    expect(execute).not.toHaveBeenCalled();
    expect(store.getState().notice).toContain("已取消");
  });
});

async function connectPreview(
  store: ReturnType<typeof createWorkbenchStore>,
) {
  await store.getState().connectDataSource({
    connectorId: "ordadb-native",
    connectorKind: "sql",
    commandLanguage: "postgresql-sql",
    dialect: "postgresql",
    endpoint: "preview",
    database: "ordadb_preview",
    credentialId: "preview-test",
    username: "dba",
    tlsMode: "disable",
  });
}

function nativeDataSourceValues(): DataSourceValues {
  return {
    connectorId: "ordadb-native",
    connectorKind: "sql",
    commandLanguage: "postgresql-sql",
    dialect: "postgresql",
    endpoint: "127.0.0.1:54329",
    adminEndpoint: "http://127.0.0.1:9080",
    database: "ordadb",
    credentialId: "ordadb-local",
    username: "ordadb_admin",
    tlsMode: "disable",
  };
}

function readyProbe(): ConnectionProbe {
  return {
    ready: true,
    bootstrapTicket: null,
    stages: [
      "service",
      "pgPort",
      "adminApi",
      "initialization",
      "authentication",
      "catalog",
    ].map((stage) => ({
      stage: stage as ConnectionProbe["stages"][number]["stage"],
      status: "passed",
      error: null,
    })),
  };
}

function bootstrapRequiredProbe(): ConnectionProbe {
  return {
    ready: false,
    bootstrapTicket: {
      ticket: "local-bootstrap-ticket",
      expiresInMs: 120_000,
    },
    stages: [
      { stage: "service", status: "passed", error: null },
      { stage: "pgPort", status: "passed", error: null },
      { stage: "adminApi", status: "passed", error: null },
      {
        stage: "initialization",
        status: "failed",
        error: {
          sqlState: "55000",
          message: "OrdaDB requires its first administrator",
          detail: null,
          hint: "complete the local administrator setup, then retry",
          position: null,
          queryId: "bootstrap-required",
        },
      },
      { stage: "authentication", status: "skipped", error: null },
      { stage: "catalog", status: "skipped", error: null },
    ],
  };
}

function externalDocument(
  path: string,
  content: string,
  modifiedAtMs: number,
): SqlDocument {
  return {
    locator: { kind: "external", path },
    path,
    name: path.split(/[\\/]/).at(-1) ?? path,
    content,
    revision: {
      sizeBytes: new TextEncoder().encode(content).byteLength,
      modifiedAtMs,
      sha256: modifiedAtMs.toString(16).padStart(64, "0"),
    },
  };
}

function dbmsTestError(sqlState: string, message: string) {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: "workbench-test",
  };
}
