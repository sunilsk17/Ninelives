import { expect, test } from "@playwright/test";

test("a protected server asks for the API token and remembers it", async ({ page }) => {
  await page.goto("/");
  const form = page.getByRole("form", { name: "Sign in" });
  await expect(form).toBeVisible();
  await expect(page.getByText("sign-in needed")).toBeVisible();

  await form.getByLabel("API token").fill("wrong");
  await form.getByRole("button", { name: "Sign in" }).click();
  await expect(form.getByRole("alert")).toHaveText("That token was not accepted.");

  await form.getByLabel("API token").fill("e2e-secret");
  await form.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: /Runs/ })).toBeVisible();
  await expect(page.getByText("live", { exact: true })).toBeVisible();

  await page.reload(); // the token is remembered in this browser
  await expect(page.getByRole("heading", { name: /Runs/ })).toBeVisible();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("form", { name: "Sign in" })).toBeVisible();
});
