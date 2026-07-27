import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { initialSql } from "./data/preview";
import {
  resetPreviewPluginManagerForTests,
  setPreviewRegistryAvailabilityForTests,
} from "./lib/pluginManager";
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
  isTauriRuntime: vi.fn().mockReturnValue(false),
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

const initialWorkbenchState = useWorkbenchStore.getState();

describe("OrdaDB workbench", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    resetPreviewPluginManagerForTests();
    useWorkbenchStore.setState(initialWorkbenchState, true);
    useWorkbenchStore.setState({
      sql: initialSql,
      notice: "准备就绪",
    });
  });

  it("renders the Windows shell and professional database islands", async () => {
    renderApp();

    expect(screen.getByText("OrdaDB")).toBeInTheDocument();
    expect(screen.queryByText("OrdaDB Local / default")).not.toBeInTheDocument();
    expect(screen.getAllByText("query_01.sql")).toHaveLength(1);
    expect(
      screen.getByRole("complementary", { name: "数据库浏览器" }),
    ).toBeVisible();
    expect(screen.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    expect(
      screen.getByRole("complementary", { name: "对象检查器" }),
    ).toBeVisible();
    const menuBar = screen.getByRole("menubar", { name: "应用菜单" });
    expect(menuBar).toBeVisible();
    expect(menuBar.closest(".titlebar")).not.toBeNull();
    expect(
      screen.getByLabelText("快捷工具").closest(".titlebar"),
    ).not.toBeNull();
    expect(document.querySelector(".command-strip")).toBeNull();
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

    expect(screen.getByText("正在接收结果")).toBeVisible();
    await waitFor(() => {
      expect(screen.getByText("WAL checkpoint overview")).toBeVisible();
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
    expect(
      await screen.findByRole("tooltip", { name: "隐藏数据库浏览器" }),
    ).toHaveTextContent("隐藏数据库浏览器");

    await user.click(schemaToggle);
    expect(
      screen.queryByRole("complementary", { name: "数据库浏览器" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "显示数据库浏览器" }),
    ).toBeVisible();

    await user.click(screen.getByRole("tab", { name: "DDL" }));
    expect(screen.getByText(/CREATE TABLE public\.documents/)).toBeVisible();

    await user.click(screen.getByRole("tab", { name: "日志" }));
    expect(screen.getByText(/不连接真实数据库/)).toBeVisible();

    await user.keyboard("{Control>}{Enter}{/Control}");
    await waitFor(() => {
      expect(screen.getByText("5 行 · 36 ms")).toBeVisible();
    });
  });

  it("switches SQL dialect hints without losing the workbench state", async () => {
    const user = userEvent.setup();
    renderApp();
    await waitFor(() => {
      expect(useWorkbenchStore.getState().catalog.length).toBeGreaterThan(0);
    });

    const dialectSelector = screen.getByRole("combobox", { name: "SQL 方言" });
    expect(dialectSelector).toHaveValue("postgresql");
    expect(screen.getByText("参数 $1", { exact: true })).toBeVisible();
    expect(screen.getByText("SQL · PostgreSQL", { exact: true })).toBeVisible();
    expect(screen.getAllByText("PREVIEW", { exact: true }).length).toBeGreaterThan(
      0,
    );

    for (const [value, label, parameter] of [
      ["mysql", "MySQL", "?"],
      ["sqlite", "SQLite", "?"],
      ["sqlServer", "SQL Server", "@p1"],
      ["postgresql", "PostgreSQL", "$1"],
    ] as const) {
      await user.selectOptions(dialectSelector, value);
      expect(dialectSelector).toHaveValue(value);
      expect(screen.getByText(`参数 ${parameter}`, { exact: true })).toBeVisible();
      expect(screen.getByText(`SQL · ${label}`, { exact: true })).toBeVisible();
    }

    await user.selectOptions(dialectSelector, "sqlServer");
    const editor = screen.getByRole("textbox", { name: "SQL 编辑器" });
    fireEvent.change(editor, {
      target: { value: "select [id] from [items] where [id] = @p1" },
    });
    await user.click(screen.getByRole("button", { name: "格式化 SQL" }));
    expect(editor).toHaveValue(
      "SELECT [id] FROM [items] WHERE [id] = @p1",
    );

    await user.click(
      screen.getByRole("button", { name: "隐藏对象检查器" }),
    );
    expect(
      screen.queryByRole("complementary", { name: "对象检查器" }),
    ).not.toBeInTheDocument();
    expect(dialectSelector).toHaveValue("sqlServer");
    expect(screen.getByText("SQL · SQL Server", { exact: true })).toBeVisible();
  });

  it("opens credential-safe data sources and live-capability operations", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("button", { name: "管理数据源" }));
    const dataSource = screen.getByRole("dialog", { name: "数据源" });
    expect(dataSource).toBeVisible();
    expect(
      within(dataSource).getByText("密码仅提交到桌面凭据库"),
    ).toBeVisible();
    expect(within(dataSource).getByText("PREVIEW fixture")).toBeVisible();
    await user.click(
      within(dataSource).getByRole("button", { name: "关闭数据源" }),
    );

    await user.click(screen.getByRole("menuitem", { name: "工具" }));
    await user.click(screen.getByRole("menuitem", { name: "会话" }));
    const operations = await screen.findByRole("dialog", {
      name: "数据库运维",
    });
    expect(within(operations).getByText("当前没有活动会话")).toBeVisible();
    await user.click(
      within(operations).getByRole("button", {
        name: "关闭数据库运维",
      }),
    );
  });

  it("renders structured DBMS errors from the query stream", async () => {
    const user = userEvent.setup();
    renderApp();

    fireEvent.change(screen.getByRole("textbox", { name: "SQL 编辑器" }), {
      target: { value: "error" },
    });
    await user.click(screen.getByRole("button", { name: /^运行/ }));

    expect(await screen.findByText(/42601 · Preview/)).toBeVisible();
    expect(screen.getByText("Fixture 数据，不连接真实数据库。")).toBeVisible();
  });

  it("manages the four signed connector fixtures with accessible lifecycle actions", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("button", { name: "管理连接插件" }));
    const dialog = await screen.findByRole("dialog", { name: "连接插件" });
    const pluginView = within(dialog);

    for (const connector of [
      "OrdaDB / PostgreSQL",
      "MySQL",
      "SQLite",
      "SQL Server",
    ]) {
      expect(pluginView.getByText(connector)).toBeVisible();
    }
    expect(pluginView.getByText("Preview 目录")).toBeVisible();
    expect(
      pluginView.getByText("Preview 不执行网络下载或文件写入"),
    ).toBeVisible();

    const download = pluginView.getByRole("button", {
      name: "下载 MySQL 连接插件",
    });
    await user.hover(download);
    expect(
      await screen.findByRole("tooltip", {
        name: "下载 MySQL 连接插件",
      }),
    ).toHaveTextContent("下载 MySQL 连接插件");
    await user.click(download);
    const cancel = await pluginView.findByRole("button", {
      name: "取消 MySQL 插件操作",
    });
    await user.click(cancel);
    expect(
      await pluginView.findByRole("button", {
        name: "下载 MySQL 连接插件",
      }),
    ).toBeVisible();

    expect(
      pluginView.getByRole("button", {
        name: "重试 SQLite 连接插件",
      }),
    ).toBeVisible();
    expect(
      pluginView.getByRole("button", {
        name: "更新 SQL Server 连接插件",
      }),
    ).toBeVisible();
    await user.click(
      pluginView.getByRole("button", {
        name: "回滚 SQL Server 连接插件",
      }),
    );
    expect(await pluginView.findByText(/已安装 v0\.9\.0/)).toBeVisible();

    await user.keyboard("{Escape}");
    expect(
      screen.queryByRole("dialog", { name: "连接插件" }),
    ).not.toBeInTheDocument();
  });

  it("fails closed when the official plugin registry is not configured", async () => {
    const user = userEvent.setup();
    setPreviewRegistryAvailabilityForTests("notConfigured");
    renderApp();

    await user.keyboard(
      "{Control>}{Alt>}{Shift>}s{/Shift}{/Alt}{/Control}",
    );
    const dialog = await screen.findByRole("dialog", { name: "连接插件" });
    expect(
      within(dialog).getByRole("status"),
    ).toHaveTextContent("插件仓库未配置");
    expect(
      within(dialog).getByRole("button", {
        name: "下载 MySQL 连接插件",
      }),
    ).toBeDisabled();
  });
});
