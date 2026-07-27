import { describe, expect, it } from "vitest";
import { PreviewDbmsClient, type DbmsClient } from "./dbmsClient";

describe("PreviewDbmsClient", () => {
  it("keeps the browser adapter explicit and streams ordered query events", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const connectionId = await client.connect({
      connectorId: "ordadb-postgresql",
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
});
