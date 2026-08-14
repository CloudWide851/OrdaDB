import { Command, Search } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  workbenchCommands,
  type WorkbenchCommandId,
} from "../data/commands";
import { usePresence } from "../lib/motion";

interface CommandPaletteProps {
  open: boolean;
  onClose: () => void;
  onCommand: (commandId: WorkbenchCommandId) => void;
}

export function CommandPalette({
  open,
  onClose,
  onCommand,
}: CommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const presence = usePresence(open);

  const filteredCommands = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return workbenchCommands;

    return workbenchCommands.filter((command) =>
      `${command.label} ${command.group} ${command.keywords ?? ""}`
        .toLowerCase()
        .includes(normalized),
    );
  }, [query]);

  useEffect(() => {
    if (!open) return;
    previousFocusRef.current = document.activeElement as HTMLElement;
    setQuery("");
    setActiveIndex(0);
    window.setTimeout(() => inputRef.current?.focus());
  }, [open]);

  if (!presence.mounted) return null;

  const close = () => {
    onClose();
    window.setTimeout(() => previousFocusRef.current?.focus());
  };

  const runCommand = (commandId: WorkbenchCommandId) => {
    onCommand(commandId);
    close();
  };

  return (
    <div
      className="command-palette-backdrop"
      data-motion-presence="panel"
      data-motion-state={presence.phase}
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        className="command-palette"
        role="dialog"
        aria-modal="true"
        aria-label="命令面板"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            close();
          } else if (event.key === "ArrowDown") {
            event.preventDefault();
            setActiveIndex((index) =>
              Math.min(index + 1, filteredCommands.length - 1),
            );
          } else if (event.key === "ArrowUp") {
            event.preventDefault();
            setActiveIndex((index) => Math.max(index - 1, 0));
          } else if (event.key === "Enter") {
            const command = filteredCommands[activeIndex];
            if (command) {
              event.preventDefault();
              runCommand(command.id);
            }
          }
        }}
      >
        <label className="command-search">
          <Search size={18} aria-hidden="true" />
          <span className="sr-only">搜索命令</span>
          <input
            ref={inputRef}
            aria-label="搜索命令"
            value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              setActiveIndex(0);
            }}
            placeholder="搜索操作或设置"
            aria-controls="command-results"
          />
          <kbd>Esc</kbd>
        </label>
        <div
          id="command-results"
          className="command-results"
          role="listbox"
          aria-label="可用命令"
        >
          {filteredCommands.map((command, index) => (
            <button
              className={`command-result ${
                activeIndex === index ? "command-result--active" : ""
              }`}
              type="button"
              role="option"
              aria-selected={activeIndex === index}
              key={command.id}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => runCommand(command.id)}
            >
              <Command size={16} aria-hidden="true" />
              <span className="command-result-label">{command.label}</span>
              <span className="command-result-group">{command.group}</span>
              {command.shortcut && <kbd>{command.shortcut}</kbd>}
            </button>
          ))}
          {filteredCommands.length === 0 && (
            <div className="command-empty">没有匹配的命令</div>
          )}
        </div>
      </section>
    </div>
  );
}
