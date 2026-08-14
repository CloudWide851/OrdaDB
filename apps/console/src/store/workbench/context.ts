import type { StoreApi } from "zustand";
import type { AiClient } from "../../lib/aiClient";
import type { ConsoleClient } from "../../lib/consoleClient";
import type { DbmsClient } from "../../lib/dbmsClient";
import type { SessionSaveController } from "./documentSupport";
import type { WorkbenchState } from "./types";

export type StoreSet = StoreApi<WorkbenchState>["setState"];
export type StoreGet = StoreApi<WorkbenchState>["getState"];

export interface WorkbenchActionContext {
  ai: AiClient;
  consoleClient: ConsoleClient;
  dbms: DbmsClient;
  get: StoreGet;
  sessionSaveController: SessionSaveController;
  set: StoreSet;
}
