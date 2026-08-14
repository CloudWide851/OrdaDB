import { Clock3, FileCode2, Search, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  workbenchCommands,
  type WorkbenchCommandId,
} from "../data/commands";
import { motionDurations, usePresence } from "../lib/motion";
import { useWorkbenchStore } from "../store/workbench";
import { IconAction } from "./IconAction";

interface QuickOpenProps {
  onCommand: (commandId: WorkbenchCommandId) => void;
}

type QuickOpenItem =
  | {
      kind: "recent";
      key: string;
      label: string;
      detail: string;
      open: () => void;
    }
  | {
      kind: "file";
      key: string;
      label: string;
      detail: string;
      open: () => void;
    }
  | {
      kind: "command";
      key: string;
      label: string;
      detail: string;
      open: () => void;
    };

export function QuickOpen({ onCommand }: QuickOpenProps) {
  const mode = useWorkbenchStore((state) => state.quickOpenMode);
  const setMode = useWorkbenchStore((state) => state.setQuickOpenMode);
  const recentFiles = useWorkbenchStore((state) => state.recentFiles);
  const workspace = useWorkbenchStore((state) => state.workspace);
  const openRecentFile = useWorkbenchStore((state) => state.openRecentFile);
  const openDocument = useWorkbenchStore((state) => state.openDocument);
  const [query, setQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const lastModeRef = useRef(mode);
  if (mode) lastModeRef.current = mode;
  const renderedMode = mode ?? lastModeRef.current;
  const presence = usePresence(Boolean(mode), {
    enterDurationMs: motionDurations.feedback,
    exitDurationMs: motionDurations.exitFeedback,
  });

  useEffect(() => {
    if (!mode) return;
    setQuery("");
    window.setTimeout(() => inputRef.current?.focus());
  }, [mode]);

  useEffect(() => {
    if (!mode) return;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      setMode(null);
    };
    window.addEventListener("keydown", closeOnEscape, true);
    return () => window.removeEventListener("keydown", closeOnEscape, true);
  }, [mode, setMode]);

  const items = useMemo(() => {
    if (!mode) return [];
    const recent: QuickOpenItem[] = recentFiles.map((entry) => ({
      kind: "recent",
      key: `recent:${locatorKey(entry.locator)}`,
      label: entry.name,
      detail:
        entry.locator.kind === "workspace"
          ? `${entry.locator.rootPath} · ${entry.locator.path}`
          : entry.locator.path,
      open: () => void openRecentFile(entry),
    }));
    const files: QuickOpenItem[] = (workspace?.entries ?? [])
      .filter((entry) => entry.kind === "sqlFile")
      .map((entry) => ({
        kind: "file",
        key: `file:${entry.path}`,
        label: entry.name,
        detail: entry.path,
        open: () => void openDocument(entry.path),
      }));
    const commands: QuickOpenItem[] = workbenchCommands.map((command) => ({
      kind: "command",
      key: `command:${command.id}`,
      label: command.label,
      detail: `${command.group}${command.shortcut ? ` · ${command.shortcut}` : ""}`,
      open: () => onCommand(command.id),
    }));
    const source =
      mode === "recent"
        ? recent
        : mode === "files"
          ? files
          : [...recent, ...files, ...commands];
    const normalized = query.trim().toLocaleLowerCase();
    return source
      .filter(
        (item) =>
          !normalized ||
          item.label.toLocaleLowerCase().includes(normalized) ||
          item.detail.toLocaleLowerCase().includes(normalized),
      )
      .slice(0, 50);
  }, [
    mode,
    onCommand,
    openDocument,
    openRecentFile,
    query,
    recentFiles,
    workspace,
  ]);

  if (!presence.mounted || !renderedMode) return null;

  const close = () => setMode(null);
  return (
    <div
      className="quick-open-backdrop"
      data-motion-presence="feedback"
      data-motion-state={presence.phase}
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        className="quick-open"
        role="dialog"
        aria-modal="true"
        aria-label={quickOpenLabel(renderedMode)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && items[0]) {
            event.preventDefault();
            items[0].open();
            close();
          }
        }}
      >
        <header>
          <Search size={16} aria-hidden="true" />
          <input
            ref={inputRef}
            aria-label={quickOpenLabel(renderedMode)}
            placeholder={quickOpenPlaceholder(renderedMode)}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <IconAction
            label="关闭快速导航"
            icon={<X size={15} aria-hidden="true" />}
            onClick={close}
          />
        </header>
        <div className="quick-open-results" role="listbox">
          {items.length === 0 ? (
            <p>没有匹配项</p>
          ) : (
            items.map((item) => (
              <button
                type="button"
                role="option"
                aria-selected="false"
                key={item.key}
                onClick={() => {
                  item.open();
                  close();
                }}
              >
                {item.kind === "recent" ? (
                  <Clock3 size={14} aria-hidden="true" />
                ) : (
                  <FileCode2 size={14} aria-hidden="true" />
                )}
                <span>
                  <strong>{item.label}</strong>
                  <small>{item.detail}</small>
                </span>
              </button>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function quickOpenLabel(mode: "recent" | "files" | "global") {
  if (mode === "recent") return "最近文件";
  if (mode === "files") return "转到文件";
  return "全局搜索";
}

function quickOpenPlaceholder(mode: "recent" | "files" | "global") {
  if (mode === "recent") return "搜索最近文件";
  if (mode === "files") return "搜索工作区文件";
  return "搜索文件、命令和最近项目";
}

function locatorKey(
  locator:
    | { kind: "workspace"; rootPath: string; path: string }
    | { kind: "external"; path: string },
) {
  return locator.kind === "workspace"
    ? `${locator.rootPath}:${locator.path}`
    : locator.path;
}
