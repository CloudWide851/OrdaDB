import {
  useEffect,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import {
  commandById,
  workbenchMenus,
  type WorkbenchCommandId,
} from "../data/commands";

interface MenuBarProps {
  onCommand: (commandId: WorkbenchCommandId) => void;
}

export function MenuBar({ onCommand }: MenuBarProps) {
  const [openMenuIndex, setOpenMenuIndex] = useState<number | null>(null);
  const [focusedItemIndex, setFocusedItemIndex] = useState(0);
  const rootRef = useRef<HTMLDivElement>(null);
  const menuButtonRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const activeMenuRef = useRef<HTMLDivElement>(null);

  const focusMenuItem = (index: number) => {
    const items = activeMenuRef.current?.querySelectorAll<HTMLButtonElement>(
      '[role="menuitem"]:not(:disabled)',
    );
    if (!items?.length) return;

    const nextIndex = (index + items.length) % items.length;
    setFocusedItemIndex(nextIndex);
    items[nextIndex]?.focus();
  };

  const openMenu = (index: number) => {
    setOpenMenuIndex(index);
    setFocusedItemIndex(0);
    window.setTimeout(() => focusMenuItem(0));
  };

  const closeMenu = (restoreFocus = true) => {
    const previousIndex = openMenuIndex;
    setOpenMenuIndex(null);
    if (restoreFocus && previousIndex !== null) {
      menuButtonRefs.current[previousIndex]?.focus();
    }
  };

  useEffect(() => {
    const handlePointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        closeMenu(false);
      }
    };

    const handleGlobalKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Alt" && !event.ctrlKey && !event.metaKey) {
        event.preventDefault();
        setOpenMenuIndex(null);
        menuButtonRefs.current[0]?.focus();
        return;
      }

      if (event.altKey && !event.ctrlKey && !event.metaKey) {
        const menuIndex = workbenchMenus.findIndex(
          (menu) => menu.accessKey.toLowerCase() === event.key.toLowerCase(),
        );
        if (menuIndex >= 0) {
          event.preventDefault();
          menuButtonRefs.current[menuIndex]?.focus();
          openMenu(menuIndex);
        }
      }
    };

    document.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleGlobalKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleGlobalKeyDown);
    };
  }, [openMenuIndex]);

  const handleTopLevelKeyDown = (
    event: ReactKeyboardEvent<HTMLButtonElement>,
    index: number,
  ) => {
    if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
      event.preventDefault();
      const direction = event.key === "ArrowRight" ? 1 : -1;
      const nextIndex =
        (index + direction + workbenchMenus.length) % workbenchMenus.length;
      menuButtonRefs.current[nextIndex]?.focus();
      if (openMenuIndex !== null) openMenu(nextIndex);
    } else if (
      event.key === "ArrowDown" ||
      event.key === "Enter" ||
      event.key === " "
    ) {
      event.preventDefault();
      openMenu(index);
    } else if (event.key === "Escape") {
      closeMenu();
    }
  };

  const handleMenuKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      focusMenuItem(focusedItemIndex + 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      focusMenuItem(focusedItemIndex - 1);
    } else if (event.key === "Home") {
      event.preventDefault();
      focusMenuItem(0);
    } else if (event.key === "End") {
      event.preventDefault();
      focusMenuItem(-1);
    } else if (event.key === "Escape") {
      event.preventDefault();
      closeMenu();
    } else if (
      (event.key === "ArrowLeft" || event.key === "ArrowRight") &&
      openMenuIndex !== null
    ) {
      event.preventDefault();
      const direction = event.key === "ArrowRight" ? 1 : -1;
      const nextIndex =
        (openMenuIndex + direction + workbenchMenus.length) %
        workbenchMenus.length;
      menuButtonRefs.current[nextIndex]?.focus();
      openMenu(nextIndex);
    }
  };

  const activeMenu =
    openMenuIndex === null ? null : workbenchMenus[openMenuIndex];

  return (
    <div className="menu-bar-shell" ref={rootRef}>
      <nav className="menu-bar" role="menubar" aria-label="应用菜单">
        {workbenchMenus.map((menu, index) => (
          <button
            key={menu.id}
            ref={(element) => {
              menuButtonRefs.current[index] = element;
            }}
            className={`menu-trigger ${
              openMenuIndex === index ? "menu-trigger--open" : ""
            }`}
            type="button"
            role="menuitem"
            aria-haspopup="menu"
            aria-expanded={openMenuIndex === index}
            aria-keyshortcuts={`Alt+${menu.accessKey}`}
            onClick={() =>
              openMenuIndex === index ? closeMenu(false) : openMenu(index)
            }
            onMouseEnter={() => {
              if (openMenuIndex !== null && openMenuIndex !== index) {
                openMenu(index);
              }
            }}
            onKeyDown={(event) => handleTopLevelKeyDown(event, index)}
          >
            {menu.label}
          </button>
        ))}
      </nav>

      {activeMenu && (
        <div
          ref={activeMenuRef}
          className="app-menu"
          role="menu"
          aria-label={activeMenu.label}
          style={{ "--menu-index": openMenuIndex ?? 0 } as CSSProperties}
          onKeyDown={handleMenuKeyDown}
        >
          {activeMenu.items.map((item, itemIndex) => {
            if (item === "separator") {
              return (
                <div
                  className="app-menu-separator"
                  role="separator"
                  key={`${activeMenu.id}-${itemIndex}`}
                />
              );
            }

            const command = commandById.get(item);
            if (!command) return null;

            return (
              <button
                className="app-menu-item"
                type="button"
                role="menuitem"
                disabled={command.disabled}
                key={command.id}
                onClick={() => {
                  closeMenu(false);
                  onCommand(command.id);
                }}
              >
                <span>{command.label}</span>
                {command.shortcut && (
                  <span className="menu-shortcut">{command.shortcut}</span>
                )}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
