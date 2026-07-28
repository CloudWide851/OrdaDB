import { describe, expect, it } from "vitest";
import {
  defaultConsoleSettings,
  PreviewConsoleClient,
  type ConnectionProfileV1,
} from "./consoleClient";

describe("PreviewConsoleClient", () => {
  it("starts compact without an implicit project or recovery", async () => {
    const client = new PreviewConsoleClient();

    await expect(client.bootstrap()).resolves.toEqual({
      settings: defaultConsoleSettings,
      recovery: null,
      connectionProfiles: [],
    });
    const workspace = await client.pickWorkspace();
    expect(workspace).toMatchObject({
      formatVersion: 1,
      rootPath: "Preview fixture",
    });
    expect(workspace?.entries.map((entry) => entry.path)).toEqual([
      "queries",
      "queries/customers.sql",
      "scratch.sql",
    ]);
    expect((await client.bootstrap()).recovery).toBeNull();
  });

  it("provides an explicit in-memory SQL lifecycle without local file access", async () => {
    const client = new PreviewConsoleClient();
    const workspace = await client.pickWorkspace();
    const rootPath = workspace?.rootPath ?? "";
    const opened = await client.openDocument(rootPath, "scratch.sql");
    const created = await client.newDocument(rootPath, "", "query_01.sql");
    const saved = await client.saveDocument(rootPath, {
      ...created,
      content: "select 42;\n",
      savedContent: "",
      dirty: true,
      conflict: false,
    });

    expect(opened.content).toBe("select 1;\n");
    expect(saved.content).toBe("select 42;\n");
    expect(saved.revision.sha256).toMatch(/^[a-f0-9]{64}$/);
    await expect(
      client.saveDocument(rootPath, {
        ...created,
        content: "stale",
        savedContent: "",
        dirty: true,
        conflict: false,
      }),
    ).rejects.toMatchObject({ sqlState: "40001" });
    expect(JSON.stringify(await client.bootstrap())).not.toMatch(/select 42/i);
  });

  it("persists settings and credential references without accepting secrets", async () => {
    const client = new PreviewConsoleClient();
    const settings = {
      ...defaultConsoleSettings,
      uiFontSize: 10,
      reopenLastProject: true,
    };
    const profile: ConnectionProfileV1 = {
      formatVersion: 1,
      profileId: "local",
      label: "本地",
      connectorId: "ordadb-postgresql",
      dialect: "postgresql",
      endpoint: "127.0.0.1:54329",
      adminEndpoint: "http://127.0.0.1:9080",
      database: "ordadb",
      credentialId: "credential-reference",
      autoReconnect: true,
    };

    await client.saveSettings(settings);
    await client.saveConnectionProfile(profile);
    const bootstrap = await client.bootstrap();
    expect(bootstrap.settings).toEqual(settings);
    expect(bootstrap.connectionProfiles).toEqual([profile]);
    expect(JSON.stringify(bootstrap)).not.toMatch(/password|apiKey/i);
  });
});
