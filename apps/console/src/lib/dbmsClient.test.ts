import { describe, expect, it } from "vitest";
import { PreviewDbmsClient, type DbmsClient } from "./dbmsClient";

describe("PreviewDbmsClient", () => {
  it("keeps the browser adapter explicit and streams ordered query events", async () => {
    const client: DbmsClient = new PreviewDbmsClient();
    const connectionId = await client.connect({
      connectorId: "ordadb-postgresql",
      dialect: "postgresql",
      endpoint: "preview",
    });
    const events = [];

    for await (const event of client.execute(connectionId, "select 1")) {
      events.push(event);
    }

    expect(connectionId).toBe("preview-connection");
    expect(events.map((event) => event.kind)).toEqual([
      "schema",
      "batch",
      "complete",
    ]);
    expect(events[1]).toMatchObject({
      kind: "batch",
      rows: [["Preview · 不连接真实数据库"]],
    });
  });
});
