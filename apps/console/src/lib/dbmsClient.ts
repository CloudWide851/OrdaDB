import type { SqlDialect } from "../types";

export interface DbmsConnectionRequest {
  connectorId: string;
  dialect: SqlDialect;
  endpoint: string;
  database?: string;
  credentialId?: string;
}

export interface DbmsCatalogObject {
  kind: string;
  schema: string;
  name: string;
}

export type DbmsQueryEvent =
  | { kind: "schema"; columns: Array<{ name: string; type: string }> }
  | { kind: "batch"; rows: unknown[][] }
  | { kind: "progress"; rowsProcessed: number }
  | { kind: "notice"; message: string }
  | { kind: "complete"; commandTag: string };

export interface DbmsMonitorSnapshot {
  sessions: number;
  activeQueries: number;
}

export interface DbmsClient {
  connect(request: DbmsConnectionRequest): Promise<string>;
  catalog(connectionId: string): Promise<DbmsCatalogObject[]>;
  execute(
    connectionId: string,
    sql: string,
    params?: unknown[],
  ): AsyncIterable<DbmsQueryEvent>;
  cancel(requestId: string): Promise<void>;
  begin(connectionId: string): Promise<void>;
  commit(connectionId: string): Promise<void>;
  rollback(connectionId: string): Promise<void>;
  monitor(connectionId: string): Promise<DbmsMonitorSnapshot>;
}

export class PreviewDbmsClient implements DbmsClient {
  connect: DbmsClient["connect"] = async () => {
    return "preview-connection";
  };

  catalog: DbmsClient["catalog"] = async () => {
    return [{ kind: "table", schema: "public", name: "documents" }];
  };

  execute: DbmsClient["execute"] = async function* () {
    yield {
      kind: "schema",
      columns: [{ name: "preview", type: "text" }],
    };
    yield { kind: "batch", rows: [["Preview · 不连接真实数据库"]] };
    yield { kind: "complete", commandTag: "SELECT 1" };
  };

  cancel: DbmsClient["cancel"] = async () => {};
  begin: DbmsClient["begin"] = async () => {};
  commit: DbmsClient["commit"] = async () => {};
  rollback: DbmsClient["rollback"] = async () => {};

  monitor: DbmsClient["monitor"] = async () => {
    return { sessions: 0, activeQueries: 0 };
  };
}
