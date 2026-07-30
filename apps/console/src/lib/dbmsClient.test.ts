import { describe, expect, it } from "vitest";
import { PreviewDbmsClient, type DbmsClient } from "./dbmsClient";

describe("PreviewDbmsClient", () => {
  it("keeps the browser adapter explicit and streams ordered query events", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const probe = await client.probe({
      connectorId: "ordadb-native",
      dialect: "postgresql",
      endpoint: "preview",
      credentialId: "preview-test",
    });
    const connectionId = await client.connect({
      connectorId: "ordadb-native",
      dialect: "postgresql",
      endpoint: "preview",
      credentialId: "preview-test",
    });
    const operation = await client.execute(
      connectionId.connectionId,
      "select 1",
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
      username: "dba",
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
    const operation = await client.execute("preview-connection", "error");
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
