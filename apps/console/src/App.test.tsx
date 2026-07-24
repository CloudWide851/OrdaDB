import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
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
      assistantVisible: true,
      activeResultTab: "data",
      queryState: "idle",
      rows: [],
      errorMessage: null,
      durationMs: null,
    });
  });

  it("renders the professional three-pane preview shell", async () => {
    renderApp();

    expect(screen.getByText("OrdaDB")).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: "Schema 浏览器" })).toBeVisible();
    expect(screen.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    expect(screen.getByRole("complementary", { name: "AI 查询助手" })).toBeVisible();
    expect(await screen.findByText("界面预览")).toBeVisible();
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

  it("supports keyboard execution and accessible pane controls", async () => {
    const user = userEvent.setup();
    renderApp();

    const schemaToggle = screen.getByRole("button", { name: "隐藏 Schema" });
    await user.hover(schemaToggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("隐藏 Schema");

    await user.click(schemaToggle);
    expect(
      screen.queryByRole("complementary", { name: "Schema 浏览器" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "显示 Schema" })).toBeVisible();

    await act(async () => {
      await user.keyboard("{Control>}{Enter}{/Control}");
    });
    await waitFor(() => {
      expect(screen.getByText("5 行 · 36 ms")).toBeVisible();
    });
  });
});
