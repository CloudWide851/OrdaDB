import { describe, expect, it } from "vitest";
import {
  commandForKeyboardEvent,
  defaultKeybindings,
  workbenchCommands,
} from "./commands";

describe("workbench command keybindings", () => {
  it("maps the required navigation shortcuts without legacy collisions", () => {
    expect(commandForKeyboardEvent(keyboard("1", { altKey: true }))).toBe(
      "database-view",
    );
    expect(commandForKeyboardEvent(keyboard("2", { altKey: true }))).toBe(
      "files-view",
    );
    expect(commandForKeyboardEvent(keyboard("3", { altKey: true }))).toBe(
      "object-inspector",
    );
    expect(commandForKeyboardEvent(keyboard("e", { ctrlKey: true }))).toBe(
      "recent-files",
    );
    expect(
      commandForKeyboardEvent(
        keyboard("e", { ctrlKey: true, altKey: true }),
      ),
    ).toBe("explain-query");
    expect(
      commandForKeyboardEvent(
        keyboard("s", { ctrlKey: true, altKey: true, shiftKey: true }),
      ),
    ).toBe("data-sources");
    expect(
      commandForKeyboardEvent(keyboard("Home", { altKey: true })),
    ).toBe("focus-navigation");
    expect(
      commandForKeyboardEvent(
        keyboard("n", { ctrlKey: true, shiftKey: true }),
      ),
    ).toBe("go-to-file");
  });

  it("derives every declared accelerator from the command registry", () => {
    const declared = workbenchCommands
      .filter((command) => command.shortcut)
      .map((command) => [command.id, command.shortcut]);
    const bindings = defaultKeybindings.bindings.map((binding) => [
      binding.commandId,
      binding.accelerator,
    ]);

    expect(bindings).toEqual(declared);
    expect(new Set(bindings.map(([, accelerator]) => accelerator)).size).toBe(
      bindings.length,
    );
  });
});

function keyboard(
  key: string,
  init: Pick<
    KeyboardEventInit,
    "altKey" | "ctrlKey" | "metaKey" | "shiftKey"
  > = {},
) {
  return new KeyboardEvent("keydown", { key, ...init });
}
