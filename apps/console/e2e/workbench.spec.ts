import { expect, test } from "@playwright/test";

test.describe("OrdaDB SQL workbench", () => {
  test("opens, explains controls, runs preview SQL, and keeps the layout contained", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto("/");

    await expect(page.getByText("OrdaDB", { exact: true })).toBeVisible();
    await expect(
      page.getByRole("complementary", { name: "Schema 浏览器" }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "SQL 编辑器" })).toBeVisible();
    await expect(
      page.getByRole("textbox", { name: "SQL 编辑器" }),
    ).toBeVisible({ timeout: 20_000 });
    await expect(
      page.getByRole("complementary", { name: "AI 查询助手" }),
    ).toBeVisible();

    const schemaToggle = page.getByRole("button", { name: "隐藏 Schema" });
    await schemaToggle.hover();
    await expect(page.getByRole("tooltip")).toContainText("隐藏 Schema");

    await page.getByRole("button", { name: /^运行/ }).click();
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
    await expect(
      page.getByText("向量检索在事务系统中的边界"),
    ).toBeVisible();

    await page.screenshot({
      path: "test-results/ordadb-workbench.png",
      fullPage: true,
    });

    await schemaToggle.click();
    await expect(
      page.getByRole("button", { name: "显示 Schema" }),
    ).toBeVisible();

    const hasHorizontalOverflow = await page.evaluate(
      () => document.documentElement.scrollWidth > window.innerWidth,
    );
    expect(hasHorizontalOverflow).toBe(false);

  });

  test("runs a preview query with Control+Enter", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("textbox", { name: "SQL 编辑器" }).click();
    await page.keyboard.press("Control+Enter");
    await expect(page.getByText("5 行 · 36 ms")).toBeVisible();
  });
});
