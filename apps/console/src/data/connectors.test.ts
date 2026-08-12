import { describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename, extname } from "node:path";
import { fileURLToPath } from "node:url";
import connectorLogoProvenance from "./connector-logo-provenance.json";
import {
  compareSemanticVersions,
  connectorDefinitions,
  formatConnectorBytes,
  pluginConnectorDefinitions,
  projectConnectorCatalog,
  type PluginCatalogSnapshot,
} from "./connectors";

describe("connector catalog projection", () => {
  it("binds every connector to one local, immutable official logo asset", async () => {
    expect(
      connectorLogoProvenance.assets.map((asset) => asset.connectorId),
    ).toEqual(connectorDefinitions.map((connector) => connector.id));
    expect(connectorLogoProvenance.assets).toHaveLength(10);

    for (const asset of connectorLogoProvenance.assets) {
      expect(asset.vendor).not.toHaveLength(0);
      expect(asset.officialSource).toMatch(/^https:\/\//);
      expect(asset.officialSource).not.toMatch(
        /simpleicons|simple-icons|wikipedia|wikimedia/i,
      );
      expect(asset.sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(asset.retrievedAt).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(asset.upstreamRevision).not.toHaveLength(0);
      expect(asset.trademarkNotice).not.toHaveLength(0);
      const assetUrl = new URL(asset.assetPath, import.meta.url);
      const bytes = await readFile(fileURLToPath(assetUrl));
      expect(createHash("sha256").update(bytes).digest("hex")).toBe(asset.sha256);
      if (asset.assetPath.endsWith(".svg")) {
        const svg = bytes.toString("utf8");
        expect(svg).not.toMatch(/<script\b/i);
        expect(svg).not.toMatch(
          /(?:href|src)=["'](?:https?:|\/\/)(?!www\.w3\.org\/|purl\.org\/)/i,
        );
      }
    }

    for (const [index, connector] of connectorDefinitions.entries()) {
      const asset = connectorLogoProvenance.assets[index];
      expect(connector.logoUrl).not.toMatch(/^https?:\/\//);
      if (connector.logoUrl.startsWith("data:")) {
        expect(connector.logoUrl).toMatch(
          asset.assetPath.endsWith(".svg")
            ? /^data:image\/svg\+xml[,;]/
            : /^data:image\/png[,;]/,
        );
      } else {
        const extension = extname(asset.assetPath);
        const stem = basename(asset.assetPath, extension);
        expect(
          basename(decodeURIComponent(new URL(connector.logoUrl, import.meta.url).pathname)),
        ).toMatch(new RegExp(`^${stem}(?:-[A-Za-z0-9_-]+)?\\${extension}$`));
      }
    }
  });

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
