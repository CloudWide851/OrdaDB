import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

test.describe("controlled AI workbench", () => {
  test.setTimeout(60_000);

  test("streams Preview, focuses approval, cancels, and never exposes credentials", async ({
    page,
  }) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.setViewportSize({ width: 1100, height: 720 });
    await page.goto("/", { waitUntil: "commit" });
    await expect(page.locator(".app-shell")).toBeVisible({ timeout: 45_000 });

    await page.keyboard.press("Control+Alt+A");
    const ai = page.getByRole("complementary", { name: "AI 助手" });
    await expect(ai).toBeVisible();
    await expect(ai.getByText("Preview · 不执行")).toBeVisible();
    await expect(page.locator(".workbench")).toHaveClass(/workbench--ai-pane/);
    await expectNoPageOverflow(page);

    await page.keyboard.press("Control+,");
    const settings = page.getByRole("dialog", { name: "设置" });
    await settings.getByLabel("搜索设置").fill("凭据");
    await expect(settings.getByLabel("凭据引用")).toHaveCount(0);
    const credential = settings.getByLabel("AI API Key 状态");
    await expect(credential).toContainText(
      "Browser Preview 不读取或保存系统凭据",
    );
    await expect(credential.getByRole("button", { name: "设置" })).toBeDisabled();
    await settings.getByRole("button", { name: "关闭设置" }).click();

    const prompt = ai.getByRole("textbox", { name: "询问 OrdaDB AI" });
    await prompt.fill("解释当前 Schema");
    await prompt.press("Control+Enter");
    await expect(ai.getByText(/这是确定性 Browser Preview/)).toBeVisible();
    await expect(ai.getByLabel("AI 工具审计")).toContainText("未访问数据库");

    await prompt.fill("删除旧记录");
    await prompt.press("Control+Enter");
    const approval = ai.getByRole("alert", { name: "需要确认" });
    await expect(approval).toBeVisible();
    const deny = approval.getByRole("button", { name: "拒绝" });
    await expect(deny).toBeFocused();
    await deny.press("Enter");
    await expect(ai.getByText(/Preview 未执行任何数据库命令/)).toBeVisible();

    await prompt.fill("/wait");
    await prompt.press("Control+Enter");
    await ai.getByRole("button", { name: "取消" }).click();
    await expect(ai.locator(".ai-composer__actions > span")).toHaveText("已取消");

    for (const viewport of [
      { width: 1100, height: 720 },
      { width: 1440, height: 900 },
      { width: 1920, height: 1080 },
    ]) {
      await page.setViewportSize(viewport);
      await expectNoPageOverflow(page);
      await expect(ai).toBeVisible();
    }
  });
});

async function expectNoPageOverflow(page: Page) {
  await expect
    .poll(() =>
      page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    )
    .toBe(true);
}
