import type { SqlDocument } from "../lib/consoleClient";
import type { ConnectionProbe } from "../lib/dbmsClient";
import {
  PreviewAiClient,
  type AiRunOperation,
  type AiRunRequest,
} from "../lib/aiClient";
import type {
  createWorkbenchStore,
  DataSourceValues,
} from "./workbench";
export class IncompleteAiClient extends PreviewAiClient {
  override async start(request: AiRunRequest): Promise<AiRunOperation> {
    return {
      runId: request.runId,
      events: (async function* () {
        yield {
          runId: request.runId,
          sequence: 1,
          kind: "started" as const,
        };
      })(),
    };
  }
}

export async function connectPreview(
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

export function nativeDataSourceValues(): DataSourceValues {
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

export function readyProbe(): ConnectionProbe {
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

export function bootstrapRequiredProbe(): ConnectionProbe {
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

export function externalDocument(
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

export function dbmsTestError(sqlState: string, message: string) {
  return {
    sqlState,
    message,
    detail: null,
    hint: null,
    position: null,
    queryId: "workbench-test",
  };
}
