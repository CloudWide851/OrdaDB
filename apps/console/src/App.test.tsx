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
import { resetPreviewAiClientForTests } from "./lib/aiClient";
import { useWorkbenchStore } from "./store/workbench";

const tauriMocks = vi.hoisted(() => ({
  fileDropSubscribers: [] as Array<(paths: string[]) => void>,
}));

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
  subscribeFileDrops: vi.fn(
    async (listener: (paths: string[]) => void) => {
      tauriMocks.fileDropSubscribers.push(listener);
      return () => {
        const index = tauriMocks.fileDropSubscribers.indexOf(listener);
        if (index >= 0) tauriMocks.fileDropSubscribers.splice(index, 1);
      };
    },
  ),
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

async function seedPreviewWorkspace() {
  await useWorkbenchStore.getState().connectDataSource({
    connectorId: "ordadb-native",
    connectorKind: "sql",
    commandLanguage: "postgresql-sql",
    dialect: "postgresql",
    endpoint: "preview",
    database: "ordadb_preview",
    credentialId: "preview-ui",
    username: "dba",
    tlsMode: "disable",
  });
  const revision = {
    sizeBytes: initialSql.length,
    modifiedAtMs: 1,
    sha256: "a".repeat(64),
  };
  useWorkbenchStore.setState({
    workspace: {
      formatVersion: 1,
      rootPath: "C:\\Preview\\project",
      entries: [
        {
          path: "query.sql",
          name: "query.sql",
          kind: "sqlFile",
          depth: 1,
        },
      ],
    },
    documents: [
      {
        locator: {
          kind: "workspace",
          rootPath: "C:\\Preview\\project",
          path: "query.sql",
        },
        path: "query.sql",
        name: "query.sql",
        content: initialSql,
        savedContent: initialSql,
        revision,
        dirty: false,
        conflict: false,
      },
    ],
    activeDocumentPath: "query.sql",
    sql: initialSql,
    notice: "准备就绪",
  });
}

describe("OrdaDB workbench", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(async () => {
    resetPreviewPluginManagerForTests();
    resetPreviewAiClientForTests();
    tauriMocks.fileDropSubscribers.length = 0;
    useWorkbenchStore.setState(initialWorkbenchState, true);
    await useWorkbenchStore.getState().discardRecovery();
  });

  it("renders the Windows shell and professional database islands", async () => {
    renderApp();

    expect(screen.getByText("OrdaDB")).toBeInTheDocument();
    expect(screen.queryByText("OrdaDB Local / default")).not.toBeInTheDocument();
    expect(screen.queryByText("query_01.sql")).not.toBeInTheDocument();
    expect(screen.getAllByText("打开 SQL 项目").length).toBeGreaterThan(0);
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
    expect(screen.queryByText("public")).not.toBeInTheDocument();
    expect(screen.queryByText("后续能力")).not.toBeInTheDocument();
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
    await seedPreviewWorkspace();
    renderApp();

    await user.click(screen.getByRole("button", { name: /^运行/ }));

    await waitFor(() => {
      expect(screen.getByText("WAL checkpoint overview")).toBeVisible();
    });
    expect(screen.getByText("5 项 · 36 ms")).toBeVisible();
  });

  it("renders the configured NULL value without changing result data", () => {
    useWorkbenchStore.setState((state) => ({
      initialize: async () => undefined,
      settings: {
        ...state.settings,
        results: { ...state.settings.results, nullDisplay: "∅" },
      },
      queryState: "success",
      activeResultTab: "data",
      columns: [{ name: "optional_value", dataType: "text" }],
      resultBuffer: {
        pages: [{ start: 0, rows: [[null]], bytes: 4 }],
        rowCount: 1,
        totalRows: 1,
        bytes: 4,
        droppedRows: 0,
      },
      rowsProcessed: 1,
    }));

    renderApp();

    expect(screen.getByText("∅")).toHaveClass("null-value");
  });

  it("opens compact settings with 11px UI and 12px data/editor defaults", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("menuitem", { name: "文件" }));
    await user.click(screen.getByRole("menuitem", { name: /^设置/ }));
    const dialog = screen.getByRole("dialog", { name: "设置" });
    expect(within(dialog).getByLabelText("界面字体")).toHaveValue(11);
    expect(within(dialog).getByLabelText("数据字体")).toHaveValue(12);
    await user.click(
      within(dialog).getByRole("button", { name: "编辑器" }),
    );
    expect(within(dialog).getByLabelText("字号")).toHaveValue(12);
    await user.click(
      within(dialog).getByRole("button", { name: "文件与工作区" }),
    );
    expect(
      within(dialog).getByLabelText("启动时恢复上次 SQL 项目"),
    ).not.toBeChecked();
    await user.click(within(dialog).getByRole("button", { name: "外观" }));
    expect(
      within(dialog).getByLabelText("隐藏空的 Catalog 分类"),
    ).toBeChecked();
    const settingsSearch = within(dialog).getByLabelText("搜索设置");
    await user.type(settingsSearch, "模型");
    expect(within(dialog).getByLabelText("模型")).toHaveValue("gpt-5.6");
    expect(within(dialog).queryByLabelText("凭据引用")).not.toBeInTheDocument();
    expect(within(dialog).getByLabelText("AI API Key 状态")).toHaveTextContent(
      "Browser Preview 不读取或保存系统凭据",
    );
    expect(within(dialog).getByRole("button", { name: "设置" })).toBeDisabled();
    await user.clear(settingsSearch);
    await user.click(
      within(dialog).getByRole("button", { name: "保存设置" }),
    );
    expect(document.documentElement.style.getPropertyValue("--font-ui")).toBe(
      "11px",
    );
  });

  it("runs the auditable AI Preview and keeps mutations behind focused approval", async () => {
    const user = userEvent.setup();
    renderApp();

    await user.click(screen.getByRole("button", { name: "打开 AI 助手" }));
    const pane = screen.getByRole("complementary", { name: "AI 助手" });
    expect(within(pane).getByText("Preview · 不执行")).toBeVisible();
    const prompt = within(pane).getByRole("textbox", {
      name: "询问 OrdaDB AI",
    });
    await user.type(prompt, "解释当前 Schema");
    await user.click(within(pane).getByRole("button", { name: "发送" }));
    expect(
      await within(pane).findByText(/这是确定性 Browser Preview/),
    ).toBeVisible();
    expect(within(pane).getByLabelText("AI 工具审计")).toHaveTextContent(
      "未访问数据库",
    );

    await user.type(prompt, "删除旧记录");
    await user.click(within(pane).getByRole("button", { name: "发送" }));
    const approval = await within(pane).findByRole("alert", {
      name: "需要确认",
    });
    const deny = within(approval).getByRole("button", { name: "拒绝" });
    expect(deny).toHaveFocus();
    await user.click(deny);
    expect(
      await within(pane).findByText(/Preview 未执行任何数据库命令/),
    ).toBeVisible();
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
    await seedPreviewWorkspace();
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
    expect(screen.queryByText(/不连接真实数据库/)).not.toBeInTheDocument();

    await user.keyboard("{Control>}{Enter}{/Control}");
    await waitFor(() => {
      expect(screen.getByText("5 项 · 36 ms")).toBeVisible();
    });
  });

  it("switches SQL dialect hints without losing the workbench state", async () => {
    const user = userEvent.setup();
    await seedPreviewWorkspace();
    renderApp();
    await waitFor(() => {
      expect(useWorkbenchStore.getState().catalog.length).toBeGreaterThan(0);
    });

    const dialectSelector = screen.getByRole("combobox", { name: "SQL 方言" });
    expect(dialectSelector).toHaveValue("postgresql");
    expect(screen.getByText("SQL · PostgreSQL", { exact: true })).toBeVisible();

    for (const [value, label] of [
      ["mysql", "MySQL"],
      ["sqlite", "SQLite"],
      ["sqlServer", "SQL Server"],
      ["postgresql", "PostgreSQL"],
    ] as const) {
      await user.selectOptions(dialectSelector, value);
      expect(dialectSelector).toHaveValue(value);
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
    await seedPreviewWorkspace();
    renderApp();

    await user.click(screen.getByRole("tab", { name: "数据库" }));
    await user.click(screen.getByRole("button", { name: "连接数据库" }));
    const dataSource = screen.getByRole("dialog", { name: "数据源" });
    expect(dataSource).toBeVisible();
    expect(
      within(dataSource).getByText("Preview 不保存数据库密码"),
    ).toBeVisible();
    expect(within(dataSource).queryByLabelText("密码")).not.toBeInTheDocument();
    expect(within(dataSource).getByText("PREVIEW fixture")).toBeVisible();
    expect(within(dataSource).getByLabelText("服务地址")).toHaveValue(
      "127.0.0.1:54329",
    );
    expect(within(dataSource).getByLabelText("管理 API")).toBeVisible();
    expect(within(dataSource).queryByLabelText("TLS")).not.toBeInTheDocument();
    await user.click(
      within(dataSource).getByRole("button", { name: "PostgreSQL" }),
    );
    expect(within(dataSource).getByLabelText("主机与端口")).toHaveValue(
      "127.0.0.1:5432",
    );
    expect(within(dataSource).getByLabelText("TLS")).toBeVisible();
    expect(
      within(dataSource).queryByLabelText("管理 API"),
    ).not.toBeInTheDocument();
    expect(
      within(dataSource).queryByText("OrdaDB / PostgreSQL"),
    ).not.toBeInTheDocument();
    const connectorLogos = dataSource.querySelectorAll(
      ".data-source-choice img",
    );
    expect(connectorLogos).toHaveLength(10);
    for (const image of connectorLogos) {
      expect(image.getAttribute("src")).toMatch(
        /^(?:data:image\/svg\+xml|\/src\/assets\/connectors\/)/,
      );
    }
    await user.click(
      within(dataSource).getByRole("button", { name: "关闭数据源" }),
    );

    await user.click(screen.getByRole("menuitem", { name: "工具" }));
    await user.click(screen.getByRole("menuitem", { name: "会话" }));
    const operations = await screen.findByRole("dialog", {
      name: "数据库运维",
    });
    expect(within(operations).getByText("当前没有活动会话")).toBeVisible();
    await user.click(within(operations).getByRole("button", { name: "备份" }));
    expect(
      within(operations).getByText(/逻辑备份与恢复 · Preview fixture/),
    ).toBeVisible();
    const archive = within(operations).getByRole("textbox", {
      name: "逻辑归档文件",
    });
    await user.clear(archive);
    await user.type(archive, "ui-fixture.ordbak");
    await user.click(
      within(operations).getByRole("button", { name: "创建备份" }),
    );
    expect(await within(operations).findByText("ui-fixture.ordbak")).toBeVisible();
    const restore = within(operations).getByRole("button", {
      name: "恢复归档",
    });
    await user.click(restore);
    expect(
      within(operations).getByRole("button", { name: "确认恢复并替换" }),
    ).toBeVisible();
    await user.click(
      within(operations).getByRole("button", {
        name: "关闭数据库运维",
      }),
    );
  });

  it("routes required shortcuts, file drops, navigation focus, and modal ownership", async () => {
    const user = userEvent.setup();
    const explain = vi.fn().mockResolvedValue(undefined);
    const openExternalFiles = vi.fn().mockResolvedValue(undefined);
    useWorkbenchStore.setState({
      runExplain: explain,
      openExternalFiles,
      inspectorVisible: false,
    });
    renderApp();
    await waitFor(() => {
      expect(tauriMocks.fileDropSubscribers).toHaveLength(1);
    });

    expect(
      document.querySelector(".query-dot, .connection-dot"),
    ).not.toBeInTheDocument();
    expect(screen.getByLabelText("连接状态：未连接")).toBeVisible();

    await user.keyboard("{Control>}n{/Control}");
    expect(
      screen.getByRole("button", { name: /未命名-1\.sql/ }),
    ).toBeVisible();
    fireEvent.change(screen.getByRole("textbox", { name: "SQL 编辑器" }), {
      target: { value: "select id from items;" },
    });
    await user.keyboard("{Control>}{Alt>}l{/Alt}{/Control}");
    expect(useWorkbenchStore.getState().sql).toBe("SELECT id FROM items;");

    await user.keyboard("{Alt>}2{/Alt}");
    expect(useWorkbenchStore.getState().sidebarView).toBe("workspace");
    await user.keyboard("{Alt>}1{/Alt}");
    expect(useWorkbenchStore.getState().sidebarView).toBe("database");
    await user.keyboard("{Alt>}3{/Alt}");
    expect(useWorkbenchStore.getState().inspectorVisible).toBe(true);

    await user.keyboard("{Control>}e{/Control}");
    expect(
      screen.getByRole("dialog", { name: "最近文件" }),
    ).toBeVisible();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(
      screen.queryByRole("dialog", { name: "最近文件" }),
    ).not.toBeInTheDocument();

    await user.keyboard("{Control>}{Shift>}n{/Shift}{/Control}");
    expect(screen.getByRole("dialog", { name: "转到文件" })).toBeVisible();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(
      screen.queryByRole("dialog", { name: "转到文件" }),
    ).not.toBeInTheDocument();

    await user.keyboard("{Alt>}{Home}{/Alt}");
    expect(screen.getByRole("navigation", { name: "导航栏" })).toHaveFocus();

    await user.keyboard("{Control>}{Alt>}e{/Alt}{/Control}");
    expect(explain).toHaveBeenCalledTimes(1);

    await user.keyboard(
      "{Control>}{Alt>}{Shift>}s{/Shift}{/Alt}{/Control}",
    );
    const dataSource = screen.getByRole("dialog", { name: "数据源" });
    const documentCount = useWorkbenchStore.getState().documents.length;
    await user.keyboard("{Control>}n{/Control}");
    expect(useWorkbenchStore.getState().documents).toHaveLength(documentCount);
    await user.click(
      within(dataSource).getByRole("button", { name: "关闭数据源" }),
    );

    await user.keyboard("{Shift}{Shift}");
    expect(screen.getByRole("dialog", { name: "全局搜索" })).toBeVisible();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(
      screen.queryByRole("dialog", { name: "全局搜索" }),
    ).not.toBeInTheDocument();

    tauriMocks.fileDropSubscribers[0]([
      "C:\\SQL\\dropped.sql",
      "C:\\SQL\\ignored.txt",
    ]);
    expect(openExternalFiles).toHaveBeenCalledWith([
      "C:\\SQL\\dropped.sql",
      "C:\\SQL\\ignored.txt",
    ]);
  });

  it("renders structured DBMS errors from the query stream", async () => {
    const user = userEvent.setup();
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await seedPreviewWorkspace();
    renderApp();

    fireEvent.change(screen.getByRole("textbox", { name: "SQL 编辑器" }), {
      target: { value: "error" },
    });
    await user.click(screen.getByRole("button", { name: /^运行/ }));

    expect(await screen.findByText(/42601 · Preview/)).toBeVisible();
    expect(
      screen.queryByText("Fixture 数据，不连接真实数据库。"),
    ).not.toBeInTheDocument();
  });

  it("manages the nine signed connector fixtures with accessible lifecycle actions", async () => {
    const user = userEvent.setup();
    renderApp();

    const dialog = await openPluginManagerFromDataSources(user);
    const pluginView = within(dialog);

    for (const connector of [
      "PostgreSQL",
      "MySQL",
      "SQLite",
      "SQL Server",
      "MongoDB",
      "Redis",
      "MariaDB",
      "ClickHouse",
      "Oracle",
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

    const dialog = await openPluginManagerFromDataSources(user);
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

async function openPluginManagerFromDataSources(
  user: ReturnType<typeof userEvent.setup>,
) {
  await user.keyboard(
    "{Control>}{Alt>}{Shift>}s{/Shift}{/Alt}{/Control}",
  );
  const sources = await screen.findByRole("dialog", { name: "数据源" });
  await user.click(
    within(sources).getByRole("button", { name: "PostgreSQL" }),
  );
  await user.click(
    within(sources).getByRole("button", { name: "连接插件" }),
  );
  return screen.findByRole("dialog", { name: "连接插件" });
}
