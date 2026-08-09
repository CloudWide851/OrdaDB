import { describe, expect, it } from "vitest";
import {
  defaultConnectorDescriptors,
  defaultConsoleSettings,
  PreviewConsoleClient,
  type ConnectionProfileV3,
} from "./consoleClient";

describe("PreviewConsoleClient", () => {
  it("starts compact without an implicit project or recovery", async () => {
    const client = new PreviewConsoleClient();

    await expect(client.bootstrap()).resolves.toEqual({
      settings: defaultConsoleSettings,
      recovery: null,
      recentFiles: [],
      connectionProfiles: [],
      connectorDescriptors: defaultConnectorDescriptors,
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
      appearance: {
        ...defaultConsoleSettings.appearance,
        uiFontSize: 10,
      },
      files: {
        ...defaultConsoleSettings.files,
        reopenLastProject: true,
      },
    };
    const profile: ConnectionProfileV3 = {
      formatVersion: 3,
      profileId: "local",
      label: "本地",
      dataSourceKind: "ordadbNative",
      connectorId: "ordadb-native",
      connectorKind: "sql",
      commandLanguage: "postgresql-sql",
      dialect: "postgresql",
      endpoint: "127.0.0.1:54329",
      adminEndpoint: "http://127.0.0.1:9080",
      database: "ordadb",
      tlsMode: "disable",
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
