import { describe, expect, it } from "vitest";
import {
  compareSemanticVersions,
  connectorDefinitions,
  formatConnectorBytes,
  pluginConnectorDefinitions,
  projectConnectorCatalog,
  type PluginCatalogSnapshot,
} from "./connectors";

describe("connector catalog projection", () => {
  it("keeps the nine approved connection types when the registry is absent", () => {
    const snapshot: PluginCatalogSnapshot = {
      registry: {
        availability: "notConfigured",
        apiVersion: 1,
        message: "插件仓库未配置",
      },
      plugins: [],
    };

    const projected = projectConnectorCatalog(snapshot);

    expect(projected.map((connector) => connector.id)).toEqual(
      pluginConnectorDefinitions.map((connector) => connector.id),
    );
    expect(projected.every((connector) => connector.lifecycle === "unavailable")).toBe(
      true,
    );
  });

  it("keeps native OrdaDB separate from the nine signed helpers", () => {
    expect(connectorDefinitions.map((connector) => connector.id)).toEqual([
      "ordadb-native",
      "postgresql",
      "mysql",
      "sqlite",
      "sql-server",
      "mongodb",
      "redis",
      "mariadb",
      "clickhouse",
      "oracle",
    ]);
    expect(pluginConnectorDefinitions).toHaveLength(9);
    expect(
      pluginConnectorDefinitions.some(
        (connector) => connector.id === "ordadb-native",
      ),
    ).toBe(false);
    expect(new Set(connectorDefinitions.map((connector) => connector.id)).size).toBe(
      10,
    );
    expect(
      connectorDefinitions.map((connector) => [
        connector.id,
        connector.connectorKind,
        connector.commandLanguage,
        connector.sqlDialect ?? null,
      ]),
    ).toEqual(
      expect.arrayContaining([
        ["mongodb", "document", "mongodb-json", null],
        ["redis", "keyValue", "redis-resp3", null],
        ["mariadb", "sql", "mariadb-sql", "mariadb"],
        ["clickhouse", "sql", "clickhouse-sql", "clickhouse"],
        ["oracle", "sql", "oracle-sql", "oracle"],
      ]),
    );
  });

  it("formats sizes and compares semantic versions numerically", () => {
    expect(formatConnectorBytes(8_388_608)).toBe("8.0 MB");
    expect(formatConnectorBytes(-1)).toBe("—");
    expect(compareSemanticVersions("10.0.0", "2.0.0")).toBe(1);
    expect(compareSemanticVersions("2.0.0-beta.2", "2.0.0-beta.10")).toBe(-1);
    expect(compareSemanticVersions("2.0.0", "2.0.0-beta.10")).toBe(1);
  });
});
