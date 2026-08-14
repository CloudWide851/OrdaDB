import { getSqlDialect } from "../../data/dialects";
import type { WorkbenchActionContext } from "./context";
import { catalogObjectIdentity } from "./databaseSupport";
import type { WorkbenchState } from "./types";

export function createUiActions({
  get,
  set,
}: WorkbenchActionContext) {
  return {  setDialect: (dialect) =>
    set({
      dialect,
      notice: `SQL 方言 · ${getSqlDialect(dialect).label}${
        get().connection?.mode === "preview" ? " · Preview" : ""
      }`,
    }),
  setSidebarView: (sidebarView) => set({ sidebarView }),
  setQuickOpenMode: (quickOpenMode) => set({ quickOpenMode }),
  setSchemaVisible: (schemaVisible) => set({ schemaVisible }),
  setInspectorVisible: (inspectorVisible) => set({ inspectorVisible }),
  toggleSchema: () => set((state) => ({ schemaVisible: !state.schemaVisible })),
  toggleInspector: () =>
    set((state) => ({ inspectorVisible: !state.inspectorVisible })),
  setActiveResultTab: (activeResultTab) => set({ activeResultTab }),
  setActiveInspectorTab: (activeInspectorTab) => set({ activeInspectorTab }),
  setInspectorMode: (inspectorMode) =>
    set({ inspectorMode, inspectorVisible: true }),
  setSelectedObject: (identifier) => {
    const selected =
      get().catalog.find((object) => object.id === identifier) ??
      get().catalog.find((object) => object.name === identifier) ??
      null;
    set({
      selectedObject: selected ? catalogObjectIdentity(selected) : "",
      selectedCatalogObject: selected,
    });
  },
  setCommandPaletteOpen: (commandPaletteOpen) => set({ commandPaletteOpen }),
  setPluginManagerOpen: (pluginManagerOpen) => set({ pluginManagerOpen }),
  setDataSourceOpen: (dataSourceOpen) => set({ dataSourceOpen }),
  setOperationsOpen: (operationsOpen) => set({ operationsOpen }),
  setNotice: (notice) => set({ notice }),
  } satisfies Partial<WorkbenchState>;
}
