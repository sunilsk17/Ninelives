import { expect, test } from "@playwright/test";

test("runs list loads, filters by status and has no horizontal scroll", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  await page.request.post("/api/workflows/expense", { data: { args: ["smoke", 10] } });
  await page.request.post("/api/workflows/expense", { data: { args: ["smoke", 5000] } });
  await page.goto("/");
  await expect(page.getByText("live", { exact: true })).toBeVisible();
  await expect(page.getByTestId("run-row").first()).toBeVisible();
  await page.getByRole("button", { name: /waiting/ }).click();
  const rows = page.getByTestId("run-row");
  await expect(rows.first()).toBeVisible();
  for (const s of await rows.getByTestId("status").allTextContents()) expect(s).toBe("waiting");
  await page.getByRole("button", { name: "Clear filter" }).click();
  await page.getByRole("button", { name: /Switch to (dark|light) theme/ }).click();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  expect(errors).toEqual([]);
});
