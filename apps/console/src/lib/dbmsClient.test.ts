import { describe, expect, it } from "vitest";
import { PreviewDbmsClient, type DbmsClient } from "./dbmsClient";

describe("PreviewDbmsClient", () => {
  it("keeps the browser adapter explicit and streams ordered query events", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const probe = await client.probe({
      connectorId: "ordadb-native",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "preview",
      tlsMode: "disable",
      credentialId: "preview-test",
    });
    const connectionId = await client.connect({
      connectorId: "ordadb-native",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "preview",
      tlsMode: "disable",
      credentialId: "preview-test",
    });
    const operation = await client.execute(
      connectionId.connectionId,
      {
        kind: "text",
        languageId: "postgresql-sql",
        text: "select 1",
        params: [],
      },
    );
    const events = [];

    for await (const event of operation.events) {
      events.push(event);
    }

    expect(probe.ready).toBe(true);
    expect(probe.stages).toHaveLength(6);
    expect(probe.stages.every((stage) => stage.status === "skipped")).toBe(true);
    await expect(
      client.promptCredential({
        credentialId: "preview-test",
        connectorId: "ordadb-native",
        suggestedUsername: "dba",
      }),
    ).resolves.toEqual({
      credentialId: "preview-test",
    });
    expect(connectionId.connectionId).toBe("preview-connection");
    expect(operation.requestId).toMatch(/^preview-/);
    expect(events.map((event) => event.kind)).toEqual([
      "schema",
      "batch",
      "progress",
      "notice",
      "complete",
    ]);
    expect(events[1]).toMatchObject({
      kind: "batch",
      rows: expect.arrayContaining([
        expect.arrayContaining(["WAL checkpoint overview"]),
      ]),
    });
  });

  it("preserves structured Preview failures without pretending they are live", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const operation = await client.execute("preview-connection", {
      kind: "text",
      languageId: "postgresql-sql",
      text: "error",
      params: [],
    });
    const events = [];

    for await (const event of operation.events) events.push(event);

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      kind: "error",
      error: {
        sqlState: "42601",
        message: expect.stringContaining("Preview"),
      },
    });
  });

  it("keeps document and key/value Preview results out of the SQL row path", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const document = await client.execute("preview-connection", {
      kind: "document",
      languageId: "mongodb-json",
      document: { operation: "find", collection: "items" },
    });
    const documents = [];
    for await (const event of document.events) documents.push(event);

    const keyValue = await client.execute("preview-connection", {
      kind: "arguments",
      languageId: "redis-resp3",
      arguments: ["GET", "customer:1"],
    });
    const keyValues = [];
    for await (const event of keyValue.events) keyValues.push(event);

    expect(documents.map((event) => event.kind)).toEqual([
      "documents",
      "progress",
      "complete",
    ]);
    expect(keyValues.map((event) => event.kind)).toEqual([
      "keyValues",
      "progress",
      "complete",
    ]);
    expect(documents).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ kind: "batch" })]),
    );
    expect(keyValues).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ kind: "batch" })]),
    );
  });

  it("projects administration jobs and service status as explicit Preview fixtures", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const started = await client.startOperation({
      connectionId: "preview-connection",
      kind: "backup",
      path: "fixture.ordbak",
    });

    expect(started).toMatchObject({
      kind: "backup",
      state: "queued",
      path: "fixture.ordbak",
    });
    await expect(client.operations("preview-connection")).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          operationId: started.operationId,
          state: "succeeded",
        }),
      ]),
    );
    await expect(client.service("preview-connection")).resolves.toMatchObject({
      name: "OrdaDB Preview",
      processRunning: false,
      dataDir: "Preview fixture",
    });
  });
});
