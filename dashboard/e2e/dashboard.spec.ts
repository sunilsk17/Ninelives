import { expect, test, type Page } from "@playwright/test";

async function startFromForm(page: Page, workflow: string, args: string, id: string) {
  await page.goto("/");
  await page.getByRole("button", { name: "Start a run" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Workflow").selectOption(workflow);
  await dialog.getByLabel("Arguments (JSON list)").fill(args);
  await dialog.getByLabel(/Run id/).fill(id);
  await dialog.getByRole("button", { name: "Start" }).click();
  await expect(page.getByTestId("run-id")).toHaveText(id);
}

const status = (page: Page) => page.getByTestId("status").first();

test("a checkout started from the dashboard survives the flaky payment provider and finishes", async ({ page }) => {
  await startFromForm(page, "checkout", '["order-e2e", 4999]', "checkout-e2e");
  await expect(status(page)).toHaveText("succeeded", { timeout: 20_000 });
  const entries = page.getByTestId("entry");
  await expect(entries).toHaveCount(6);
  await expect(entries.nth(1)).toContainText("charge_card");
  await expect(entries.nth(4)).toContainText("slept");
  await expect(page.getByRole("region", { name: "Output" })).toContainText('"status": "shipped"');
});

test("an expense waits for a human signal sent from the dashboard", async ({ page }) => {
  await startFromForm(page, "expense", '["dana", 1200]', "expense-e2e");
  await expect(status(page)).toHaveText("waiting");
  await expect(page.getByTestId("entry")).toContainText("decision");
  const form = page.getByRole("form", { name: "Send a signal" });
  await form.getByLabel("Signal payload").fill('{"approved": true, "by": "sam"}');
  await form.getByRole("button", { name: 'Send "decision"' }).click();
  await expect(status(page)).toHaveText("succeeded");
  await expect(page.getByRole("region", { name: "Output" })).toContainText('"approved_by": "sam"');
  await expect(page.getByRole("region", { name: "Signals" })).toContainText("decision");
});

test("the start form rejects malformed JSON", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Start a run" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Arguments (JSON list)").fill("[oops");
  await dialog.getByRole("button", { name: "Start" }).click();
  await expect(dialog.getByRole("alert")).toHaveText("Arguments is not valid JSON");
  await dialog.getByLabel("Arguments (JSON list)").fill('{"a": 1}');
  await dialog.getByRole("button", { name: "Start" }).click();
  await expect(dialog.getByRole("alert")).toHaveText("Arguments must be a JSON array");
  await dialog.getByRole("button", { name: "Cancel" }).click();
  await expect(dialog).toBeHidden();
});

test("an agent's refund waits for approval and a denial reaches the agent", async ({ page, request }) => {
  const res = await request.post("/api/workflows/agent:support", {
    data: { args: ["Please refund order A-1001, it arrived broken"], id: "agent-e2e" },
  });
  expect(res.status()).toBe(201);
  await page.goto("/#/runs/agent-e2e");
  const card = page.getByTestId("approval");
  await expect(card).toContainText("The agent wants to call refund");
  await expect(card).toContainText('"amount_cents": 4999');
  await card.getByPlaceholder("Note for the agent").fill("needs a photo first");
  await card.getByRole("button", { name: "Deny" }).click();
  await expect(status(page)).toHaveText("succeeded");
  await expect(card).toBeHidden();
  const output = page.getByRole("region", { name: "Output" });
  await expect(output).toContainText("wasn't able to refund");
  await expect(output).toContainText("needs a photo first");
  await expect(page.getByTestId("timeline")).toContainText("→ refund(");
});

test("re-running from a step forks the run and reuses earlier steps", async ({ page }) => {
  await page.goto("/#/runs/checkout-e2e");
  await expect(status(page)).toHaveText("succeeded");
  const third = page.getByTestId("entry").nth(2);
  await third.hover();
  await third.getByRole("button", { name: "re-run from here" }).click();
  await expect(page.getByTestId("run-id")).toHaveText(/^checkout-fork-/);
  await expect(page.getByText("forked from")).toBeVisible();
  await expect(status(page)).toHaveText("succeeded", { timeout: 20_000 });
  await expect(page.getByTestId("entry").first()).toContainText("copied from parent run");
  await expect(page.getByTestId("entry").nth(2)).not.toContainText("copied from parent run");
  await page.getByRole("link", { name: "checkout-e2e" }).click();
  await expect(page.getByTestId("run-id")).toHaveText("checkout-e2e");
});

test("cancelling a waiting run", async ({ page }) => {
  await startFromForm(page, "expense", '["kim", 9000]', "expense-cancel-e2e");
  await expect(status(page)).toHaveText("waiting");
  await page.getByRole("button", { name: "Cancel run" }).click();
  await expect(status(page)).toHaveText("cancelled");
  await expect(page.getByRole("button", { name: "Cancel run" })).toBeHidden();
  await expect(page.getByRole("region", { name: "Error" })).toContainText("cancelled by request");
});

test("unknown runs show a clear error", async ({ page }) => {
  await page.goto("/#/runs/does-not-exist");
  await expect(page.getByRole("alert")).toContainText("workflow does-not-exist not found");
});
