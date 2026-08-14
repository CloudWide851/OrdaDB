import { normalizeDbmsError } from "../../lib/dbmsClient";
import { normalizeAiError } from "../../lib/aiClient";
import { applyAiPersistenceProjection } from "./aiActions";
import { connectProfile } from "./connectionActions";
import type { WorkbenchActionContext } from "./context";
import {
  applyConsoleSettings,
  emptyWorkspaceSession,
} from "./documentSupport";
import type { WorkbenchState } from "./types";

export function createCoreActions({
  ai,
  consoleClient,
  dbms,
  get,
  set,
}: WorkbenchActionContext) {
  return {  initialize: async () => {
    try {
      const bootstrap = await consoleClient.bootstrap();
      set({
        settings: bootstrap.settings,
        recovery: bootstrap.recovery,
        recentFiles: bootstrap.recentFiles,
        connectionProfiles: bootstrap.connectionProfiles,
        connectorDescriptors: bootstrap.connectorDescriptors,
      });
      applyConsoleSettings(bootstrap.settings);
      if (
        bootstrap.recovery &&
        (bootstrap.settings.files.recoveryPolicy === "automatic" ||
          bootstrap.settings.files.reopenLastProject)
      ) {
        await get().restoreRecovery();
      } else if (
        bootstrap.recovery &&
        bootstrap.settings.files.recoveryPolicy === "never"
      ) {
        set({ recovery: null });
        await consoleClient.saveSession(emptyWorkspaceSession());
      }
      const reconnect = bootstrap.connectionProfiles.find(
        (profile) =>
          profile.connectorId === "ordadb-native" && profile.autoReconnect,
      );
      if (
        reconnect &&
        bootstrap.settings.connections.autoReconnectLocal &&
        dbms.mode === "desktop"
      ) {
        await connectProfile(reconnect, dbms, consoleClient, set, get);
      }
    } catch (error) {
      const normalized = normalizeDbmsError(error);
      set({
        notice: normalized.message,
      });
    }
    try {
      applyAiPersistenceProjection(set, await ai.state());
    } catch (error) {
      set({ aiError: normalizeAiError(error) });
    }
  },
  } satisfies Partial<WorkbenchState>;
}
