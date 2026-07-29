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
  it("keeps the four approved connection types when the registry is absent", () => {
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

  it("keeps native OrdaDB separate from the four signed helpers", () => {
    expect(connectorDefinitions.map((connector) => connector.id)).toEqual([
      "ordadb-native",
      "postgresql",
      "mysql",
      "sqlite",
      "sql-server",
    ]);
    expect(pluginConnectorDefinitions).toHaveLength(4);
    expect(
      pluginConnectorDefinitions.some(
        (connector) => connector.id === "ordadb-native",
      ),
    ).toBe(false);
  });

  it("formats sizes and compares semantic versions numerically", () => {
    expect(formatConnectorBytes(8_388_608)).toBe("8.0 MB");
    expect(formatConnectorBytes(-1)).toBe("—");
    expect(compareSemanticVersions("10.0.0", "2.0.0")).toBe(1);
    expect(compareSemanticVersions("2.0.0-beta.2", "2.0.0-beta.10")).toBe(-1);
    expect(compareSemanticVersions("2.0.0", "2.0.0-beta.10")).toBe(1);
  });
});
