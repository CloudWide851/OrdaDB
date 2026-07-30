import { ChevronRight, FolderTree, Search } from "lucide-react";
import { useWorkbenchStore } from "../store/workbench";

export function NavigationBar() {
  const workspace = useWorkbenchStore((state) => state.workspace);
  const documents = useWorkbenchStore((state) => state.documents);
  const activeDocumentPath = useWorkbenchStore(
    (state) => state.activeDocumentPath,
  );
  const setQuickOpenMode = useWorkbenchStore(
    (state) => state.setQuickOpenMode,
  );
  const active = documents.find(
    (document) => document.path === activeDocumentPath,
  );

  return (
    <nav
      className="navigation-bar"
      aria-label="导航栏"
      tabIndex={-1}
      data-navigation-bar
    >
      <FolderTree size={13} aria-hidden="true" />
      <span>{workspace?.rootPath ?? "独立 SQL 文件"}</span>
      {active && (
        <>
          <ChevronRight size={12} aria-hidden="true" />
          <strong>{active.name}</strong>
        </>
      )}
      <button
        type="button"
        onClick={() => setQuickOpenMode("global")}
        aria-label="打开全局搜索"
      >
        <Search size={13} aria-hidden="true" />
        搜索
        <kbd>Shift Shift</kbd>
      </button>
    </nav>
  );
}
