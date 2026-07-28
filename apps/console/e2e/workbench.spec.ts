import { expect, test, type Page } from "@playwright/test";

test.describe("OrdaDB SQL workbench", () => {
  test.setTimeout(60_000);

  test("opens the Windows shell, navigates menus, and runs preview SQL", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto("/");

    await expect(page.getByText("OrdaDB", { exact: true })).toBeVisible();
    await expect(page.getByText("界面预览", { exact: true })).toBeVisible();
    await expect(page.locator(".brand-logo")).toBeVisible();
    await expect(page.locator(".titlebar")).toHaveCSS("height", "38px");
    await expect(page.locator(".command-strip")).toHaveCount(0);
    await expect(page.getByText("OrdaDB Local / default")).toHaveCount(0);
    await expect(page.getByText("query_01.sql", { exact: true })).toHaveCount(0);
    await expect(page.locator(".query-tab")).toHaveCount(0);
    await expect(page.locator("body")).toHaveCSS("font-size", "11px");
    await expect(page.locator(".island").first()).toHaveCSS(
      "border-radius",
      "7px",
    );
    await expect(
      page.getByRole("complementary", { name: "数据库浏览器" }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    await expect(page.getByText("public", { exact: true })).toHaveCount(0);
    await expect(page.getByText("后续能力", { exact: true })).toHaveCount(0);
    await page.getByRole("tab", { name: "数据库" }).click();
    const disconnectedTree = page.locator(".schema-tree--disconnected");
    await expect(disconnectedTree.getByRole("button")).toHaveCount(1);
    await expect(
      disconnectedTree.getByRole("button", { name: "连接数据库" }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "项目" }).click();
    await openPreviewWorkspace(page);
    await expect(
      page.getByRole("textbox", { name: "SQL 编辑器" }),
    ).toBeVisible({ timeout: 45_000 });
    await expect(page.locator(".view-lines")).toHaveCSS("font-size", "12px");
    await page
      .locator(".schema-pane .heading-actions")
      .getByRole("button", { name: "新建 SQL 文件" })
      .click();
    await expect(page.getByRole("tab", { name: "query.sql" })).toBeVisible();
    const sqlEditor = page.getByRole("textbox", { name: "SQL 编辑器" });
    await sqlEditor.focus();
    await page.keyboard.press("Control+A");
    await page.keyboard.insertText("select 42;");
    await page.keyboard.press("Control+S");
    await expect(page.getByText("全部 SQL 文件已保存")).toBeVisible();
    await page
      .locator(".workspace-tree")
      .getByRole("button", { name: "query.sql" })
      .click();
    await page.getByRole("button", { name: "重命名项目条目" }).click();
    const renameInput = page.getByRole("textbox", { name: "重命名 query.sql" });
    await renameInput.fill("renamed.sql");
    await renameInput.press("Enter");
    await expect(page.getByRole("tab", { name: "renamed.sql" })).toBeVisible();
    await page
      .locator(".workspace-tree")
      .getByRole("button", { name: /renamed\.sql/ })
      .click();
    page.once("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "移入回收站" }).click();
    await expect(page.getByRole("tab", { name: "renamed.sql" })).toHaveCount(0);
    await page
      .locator(".workspace-tree")
      .getByRole("button", { name: /scratch\.sql/ })
      .click();
    await expect(
      page.getByRole("complementary", { name: "对象检查器" }),
    ).toBeVisible();
    await expect(page.locator(".island")).toHaveCount(3);
    const dialectSelector = page.getByRole("combobox", { name: "SQL 方言" });
    await expect(dialectSelector).toHaveValue("postgresql");
    await dialectSelector.hover();
    await expect(
      page.getByRole("tooltip", { name: "SQL 方言 · 参数 $1" }),
    ).toBeVisible();
    await dialectSelector.selectOption("mysql");
    await expect(page.getByText("SQL · MySQL", { exact: true })).toBeVisible();
    await dialectSelector.selectOption("sqlite");
    await expect(dialectSelector).toHaveValue("sqlite");
    await dialectSelector.selectOption("sqlServer");
    await expect(page.getByText("SQL · SQL Server", { exact: true })).toBeVisible();
    await dialectSelector.selectOption("postgresql");

    const windowControls = page.locator(".window-controls button");
    await expect(windowControls).toHaveCount(3);
    await expect(windowControls.nth(0)).toHaveAttribute(
      "aria-label",
      "最小化窗口",
    );
    await expect(windowControls.nth(1)).toHaveAttribute(
      "aria-label",
      "最大化或还原窗口",
    );
    await expect(windowControls.nth(2)).toHaveAttribute(
      "aria-label",
      "关闭窗口",
    );

    await page.keyboard.press("Alt");
    await expect(page.getByRole("menuitem", { name: "文件" })).toBeFocused();
    await page.keyboard.press("ArrowRight");
    await expect(page.getByRole("menuitem", { name: "编辑" })).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(page.getByRole("menu", { name: "编辑" })).toBeVisible();
    await expect(page.getByRole("menu", { name: "编辑" })).toHaveCSS(
      "animation-duration",
      "0.13s",
    );
    await expect(
      page.getByRole("menuitem", { name: "格式化 SQL" }),
    ).toBeVisible();
    await page.keyboard.press("Escape");

    await page.keyboard.press("Control+Shift+P");
    await expect(page.getByRole("dialog", { name: "命令面板" })).toBeVisible();
    await page.getByRole("textbox", { name: "搜索命令" }).fill("服务");
    await expect(page.getByRole("option", { name: /服务管理/ })).toBeVisible();
    await page.keyboard.press("Escape");

    await page.keyboard.press("Control+Alt+S");
    const settings = page.getByRole("dialog", { name: "设置" });
    await expect(settings.getByLabel("界面字体")).toHaveValue("11");
    await expect(settings.getByLabel("数据字体")).toHaveValue("12");
    await expect(settings.getByLabel("编辑器字体")).toHaveValue("12");
    await expect(
      settings.getByLabel("启动时自动恢复上次 SQL 项目"),
    ).not.toBeChecked();
    await settings.getByRole("button", { name: "保存设置" }).click();
    await expect(settings).toHaveCount(0);

    await page.keyboard.press("Control+Alt+Shift+S");
    const connectorManager = page.getByRole("dialog", { name: "连接插件" });
    await expect(connectorManager).toBeVisible();
    await expect(connectorManager.getByText("Preview 目录")).toBeVisible();
    await expect(
      connectorManager.getByText("Preview 不执行网络下载或文件写入"),
    ).toBeVisible();
    for (const connector of [
      "OrdaDB / PostgreSQL",
      "MySQL",
      "SQLite",
      "SQL Server",
    ]) {
      await expect(connectorManager.getByText(connector)).toBeVisible();
    }
    await page.screenshot({
      path: "test-results/ordadb-connectors.png",
      fullPage: true,
    });
    const downloadConnector = connectorManager.getByRole("button", {
      name: "下载 MySQL 连接插件",
    });
    await downloadConnector.hover();
    await expect(
      page.getByRole("tooltip", { name: "下载 MySQL 连接插件" }),
    ).toBeVisible();
    await downloadConnector.click();
    await connectorManager
      .getByRole("button", { name: "取消 MySQL 插件操作" })
      .click();
    await expect(
      connectorManager.getByRole("button", {
        name: "重试 SQLite 连接插件",
      }),
    ).toBeVisible();
    await expect(
      connectorManager.getByRole("button", {
        name: "更新 SQL Server 连接插件",
      }),
    ).toBeVisible();
    await connectorManager
      .getByRole("button", {
        name: "回滚 SQL Server 连接插件",
      })
      .click();
    await expect(connectorManager.getByText(/已安装 v0\.9\.0/)).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(connectorManager).toHaveCount(0);

    await connectPreviewDatabase(page);
    await page.getByRole("tab", { name: "数据库" }).click();
    await expect(
      page.getByRole("button", { name: /^表/ }),
    ).toHaveAttribute("aria-expanded", "false");
    await expect(page.getByText("物化视图", { exact: true })).toHaveCount(0);

    await page.getByRole("menuitem", { name: "工具" }).click();
    await page.getByRole("menuitem", { name: "会话" }).click();
    const operations = page.getByRole("dialog", { name: "数据库运维" });
    await expect(operations).toBeVisible();
    await expect(operations.getByText("当前没有活动会话")).toBeVisible();
    await operations.getByRole("button", { name: "备份" }).click();
    await expect(
      operations.getByText("逻辑备份与恢复 · Preview fixture"),
    ).toBeVisible();
    await operations
      .getByRole("textbox", { name: "逻辑归档文件" })
      .fill("playwright.ordbak");
    await operations.getByRole("button", { name: "创建备份" }).click();
    await expect(operations.getByText("playwright.ordbak")).toBeVisible();
    await operations.getByRole("button", { name: "恢复归档" }).click();
    await expect(
      operations.getByRole("button", { name: "确认恢复并替换" }),
    ).toBeVisible();
    await operations
      .getByRole("button", { name: "关闭数据库运维" })
      .click();

    const schemaToggle = page.getByRole("button", {
      name: "隐藏数据库浏览器",
    });
    await schemaToggle.hover();
    await expect(page.getByRole("tooltip")).toContainText("隐藏数据库浏览器");

    await page.getByRole("button", { name: /^运行/ }).click();
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
    await expect(page.getByText("WAL checkpoint overview")).toBeVisible();
    await expect(page.locator(".result-table")).toHaveCSS("font-size", "12px");

    await page.getByRole("button", { name: "执行计划" }).click();
    await expect(page.getByText(/Preview Plan · Seq Scan/)).toBeVisible();
    await expect(page.locator(".result-content")).toHaveCSS(
      "animation-duration",
      "0.13s",
    );

    await page.screenshot({
      path: "test-results/ordadb-workbench.png",
      fullPage: true,
    });

    await schemaToggle.click();
    await expect(
      page.getByRole("button", { name: "显示数据库浏览器" }),
    ).toBeVisible();
    const panelAnimationDurations = await page
      .locator(".center-workspace")
      .evaluate((element) =>
        element
          .getAnimations()
          .map((animation) => animation.effect?.getTiming().duration),
      );
    expect(panelAnimationDurations).toContain(180);
    await expect(dialectSelector).toHaveValue("postgresql");
  });

  test("runs from Monaco and contains every accepted viewport", async ({
    page,
  }) => {
    for (const viewport of [
      { width: 1100, height: 720 },
      { width: 1440, height: 900 },
      { width: 1920, height: 1080 },
    ]) {
      await page.setViewportSize(viewport);
      await page.goto("/");
      const hasHorizontalOverflow = await page.evaluate(
        () => document.documentElement.scrollWidth > window.innerWidth,
      );
      expect(hasHorizontalOverflow).toBe(false);
    }

    await openPreviewWorkspace(page);
    await connectPreviewDatabase(page);
    const sqlEditor = page.getByRole("textbox", { name: "SQL 编辑器" });
    await sqlEditor.focus();
    await expect(sqlEditor).toBeFocused();
    await page.keyboard.press("Control+Enter");
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
  });

  test("reduces positional motion without removing essential query feedback", async ({
    page,
  }) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto("/");
    await openPreviewWorkspace(page);
    await connectPreviewDatabase(page);

    await page
      .getByRole("button", { name: "隐藏数据库浏览器" })
      .click();
    const panelAnimations = await page
      .locator(".center-workspace")
      .evaluate((element) => element.getAnimations().length);
    expect(panelAnimations).toBe(0);

    await page.getByRole("tab", { name: "日志" }).click();
    await expect(page.locator(".result-content")).toHaveCSS(
      "animation-duration",
      "0.001s",
    );
    await expect(page.locator(".result-content")).toHaveCSS(
      "transform",
      "none",
    );

    await page.keyboard.press("Control+Alt+Shift+S");
    await expect(page.getByRole("dialog", { name: "连接插件" })).toHaveCSS(
      "animation-duration",
      "0.001s",
    );
    await expect(page.getByRole("dialog", { name: "连接插件" })).toHaveCSS(
      "transform",
      "none",
    );
    await page.keyboard.press("Escape");

    await page.getByRole("button", { name: /^运行/ }).click();
    await expect(page.locator(".loading-orbit")).toBeVisible();
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
  });
});

async function openPreviewWorkspace(page: Page) {
  await page.getByRole("tab", { name: "项目" }).click();
  const workspaceTree = page.locator(".workspace-tree");
  const openProject = workspaceTree.getByRole("button", {
    name: "打开 SQL 项目",
  });
  if (await openProject.isVisible()) {
    await openProject.click();
  }
  await workspaceTree
    .getByRole("button", { name: /customers\.sql/ })
    .click();
  await expect(page.getByRole("tab", { name: "customers.sql" })).toBeVisible();
}

async function connectPreviewDatabase(page: Page) {
  await page.getByRole("tab", { name: "数据库" }).click();
  const disconnected = page.locator(".schema-tree--disconnected");
  if (await disconnected.isVisible()) {
    await disconnected
      .getByRole("button", { name: "连接数据库" })
      .click();
    const dialog = page.getByRole("dialog", { name: "数据源" });
    await expect(dialog.getByText("密码仅提交到桌面凭据库")).toBeVisible();
    await dialog.getByLabel("密码").fill("preview-only");
    await dialog
      .getByRole("button", { name: "连接", exact: true })
      .click();
    await expect(page.getByLabel("连接状态：connected")).toBeVisible();
    if (await dialog.isVisible()) {
      await dialog.getByRole("button", { name: "关闭数据源" }).click();
    }
  }
  await page.getByRole("tab", { name: "项目" }).click();
}
