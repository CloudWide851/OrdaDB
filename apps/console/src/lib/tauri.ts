import { invoke } from "@tauri-apps/api/core";
import { getCurrentWindow } from "@tauri-apps/api/window";
import type { AppStatus } from "../types";

export const isTauriRuntime = () => "__TAURI_INTERNALS__" in window;

export async function getAppStatus(): Promise<AppStatus> {
  if (!isTauriRuntime()) {
    return {
      name: "OrdaDB Console",
      version: "0.1.0",
      mode: "preview",
      state: "preview",
    };
  }

  return invoke<AppStatus>("get_app_status");
}

export type WindowAction = "close" | "minimize" | "toggleMaximize";

export async function runWindowAction(action: WindowAction): Promise<void> {
  if (!isTauriRuntime()) {
    return;
  }

  const appWindow = getCurrentWindow();

  if (action === "close") {
    await appWindow.close();
  } else if (action === "minimize") {
    await appWindow.minimize();
  } else {
    await appWindow.toggleMaximize();
  }
}
