import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { initialSql } from "./data/preview";
import { useWorkbenchStore } from "./store/workbench";

vi.mock("@monaco-editor/react", () => ({
  default: ({
    value,
    onChange,
  }: {
    value?: string;
    onChange?: (value?: string) => void;
  }) => (
    <textarea
      aria-label="SQL 编辑器"
      value={value}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
}));

vi.mock("./lib/tauri", () => ({
  getAppStatus: vi.fn().mockResolvedValue({
    name: "OrdaDB Console",
    version: "0.1.0",
    mode: "preview",
    state: "preview",
  }),
  runWindowAction: vi.fn().mockResolvedValue(undefined),
}));

const renderApp = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
};

describe("OrdaDB workbench", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    useWorkbenchStore.setState({
      sql: initialSql,
      schemaVisible: true,
      inspectorVisible: true,
      activeResultTab: "data",
      activeInspectorTab: "properties",
      selectedObject: "documents",
      commandPaletteOpen: false,
      notice: "准备就绪",
      queryState: "idle",
      rows: [],
      errorMessage: null,
      durationMs: null,
    });
  });

  it("renders the Windows shell and professional database islands", async () => {
    renderApp();

    expect(screen.getByText("OrdaDB")).toBeInTheDocument();
    expect(
      screen.getByRole("complementary", { name: "数据库浏览器" }),
    ).toBeVisible();
    expect(screen.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    expect(
      screen.getByRole("complementary", { name: "对象检查器" }),
    ).toBeVisible();
    expect(screen.getByRole("menubar", { name: "应用菜单" })).toBeVisible();
    expect(screen.getByText("物化视图")).toBeVisible();
    expect(await screen.findByText("界面预览")).toBeVisible();

    const controls = screen
      .getByRole("button", { name: "最小化窗口" })
      .closest(".window-controls");
    expect(
      Array.from(controls?.querySelectorAll("button") ?? []).map((button) =>
        button.getAttribute("aria-label"),
      ),
    ).toEqual(["最小化窗口", "最大化或还原窗口", "关闭窗口"]);
    expect(
      screen
        .getByRole("button", { name: "最大化或还原窗口" })
        .closest("[data-tauri-drag-region]"),
    ).toBeNull();
  });

  it("runs a preview query from the primary action", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("button", { name: /^运行/ }));

    expect(screen.getByText("正在生成预览结果")).toBeVisible();
    await waitFor(() => {
      expect(screen.getByText("向量检索在事务系统中的边界")).toBeVisible();
    });
    expect(screen.getByText("5 行 · 36 ms")).toBeVisible();
  });

  it("supports menus, command palette, and keyboard navigation", async () => {
    const user = userEvent.setup();
    renderApp();

    const menuBar = screen.getByRole("menubar", { name: "应用菜单" });
    const menuItems = within(menuBar).getAllByRole("menuitem");
    expect(menuItems.map((item) => item.textContent)).toEqual([
      "文件",
      "编辑",
      "视图",
      "导航",
      "运行",
      "数据库",
      "工具",
      "窗口",
      "帮助",
    ]);

    await user.keyboard("{Alt>}{/Alt}");
    expect(screen.getByRole("menuitem", { name: "文件" })).toHaveFocus();
    await user.keyboard("{ArrowRight}");
    expect(screen.getByRole("menuitem", { name: "编辑" })).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("menu", { name: "编辑" })).toBeVisible();
    expect(
      screen.getByRole("menuitem", { name: /^格式化 SQL/ }),
    ).toBeVisible();
    await user.keyboard("{Escape}");

    await user.keyboard("{Control>}{Shift>}p{/Shift}{/Control}");
    const palette = screen.getByRole("dialog", { name: "命令面板" });
    expect(palette).toBeVisible();
    await user.type(screen.getByRole("textbox", { name: "搜索命令" }), "备份");
    expect(screen.getByRole("option", { name: /备份与恢复/ })).toBeVisible();
    await user.keyboard("{Escape}");
    expect(
      screen.queryByRole("dialog", { name: "命令面板" }),
    ).not.toBeInTheDocument();
  });

  it("supports keyboard execution and accessible pane controls", async () => {
    const user = userEvent.setup();
    renderApp();

    const schemaToggle = screen.getByRole("button", {
      name: "隐藏数据库浏览器",
    });
    await user.hover(schemaToggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "隐藏数据库浏览器",
    );

    await user.click(schemaToggle);
    expect(
      screen.queryByRole("complementary", { name: "数据库浏览器" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "显示数据库浏览器" }),
    ).toBeVisible();

    await user.click(screen.getByRole("tab", { name: "DDL" }));
    expect(screen.getByText(/CREATE TABLE public\.documents/)).toBeVisible();

    await user.keyboard("{Control>}{Enter}{/Control}");
    await waitFor(() => {
      expect(screen.getByText("5 行 · 36 ms")).toBeVisible();
    });
  });
});
