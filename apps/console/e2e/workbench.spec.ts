import { expect, test } from "@playwright/test";

test.describe("OrdaDB SQL workbench", () => {
  test.setTimeout(60_000);

  test("opens the Windows shell, navigates menus, and runs preview SQL", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto("/");

    await expect(page.getByText("OrdaDB", { exact: true })).toBeVisible();
    await expect(page.locator(".brand-logo")).toBeVisible();
    await expect(
      page.getByRole("complementary", { name: "数据库浏览器" }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    await expect(
      page.getByRole("textbox", { name: "SQL 编辑器" }),
    ).toBeVisible({ timeout: 45_000 });
    await expect(
      page.getByRole("complementary", { name: "对象检查器" }),
    ).toBeVisible();
    await expect(page.locator(".island")).toHaveCount(3);

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
    await expect(
      page.getByRole("menuitem", { name: "格式化 SQL" }),
    ).toBeVisible();
    await page.keyboard.press("Escape");

    await page.keyboard.press("Control+Shift+P");
    await expect(page.getByRole("dialog", { name: "命令面板" })).toBeVisible();
    await page.getByRole("textbox", { name: "搜索命令" }).fill("服务");
    await expect(page.getByRole("option", { name: /服务管理/ })).toBeVisible();
    await page.keyboard.press("Escape");

    const schemaToggle = page.getByRole("button", {
      name: "隐藏数据库浏览器",
    });
    await schemaToggle.hover();
    await expect(page.getByRole("tooltip")).toContainText("隐藏数据库浏览器");

    await page.getByRole("button", { name: /^运行/ }).click();
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
    await expect(
      page.getByText("向量检索在事务系统中的边界"),
    ).toBeVisible();

    await page.getByRole("button", { name: "执行计划" }).click();
    await expect(page.getByText("Hybrid Scan")).toBeVisible();

    await page.screenshot({
      path: "test-results/ordadb-workbench.png",
      fullPage: true,
    });

    await schemaToggle.click();
    await expect(
      page.getByRole("button", { name: "显示数据库浏览器" }),
    ).toBeVisible();
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

    const sqlEditor = page.getByRole("textbox", { name: "SQL 编辑器" });
    await sqlEditor.focus();
    await expect(sqlEditor).toBeFocused();
    await page.keyboard.press("Control+Enter");
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
  });
});
