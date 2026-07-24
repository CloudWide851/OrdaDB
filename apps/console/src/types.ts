export interface AppStatus {
  name: string;
  version: string;
  mode: "desktop" | "preview";
  state: "ready" | "preview";
}

export interface QueryRow {
  id: number;
  title: string;
  category: string;
  score: number;
  updatedAt: string;
}

export type QueryState = "idle" | "running" | "success" | "error";
export type ResultTab = "data" | "logs";
