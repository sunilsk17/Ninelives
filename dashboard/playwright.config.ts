import { defineConfig, devices } from "@playwright/test";

const PORT = 8766;

// Runs the real server (API + dashboard + an in-process worker) on a throwaway SQLite file.
export default defineConfig({
  testDir: "e2e",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  use: { baseURL: `http://127.0.0.1:${PORT}`, trace: "retain-on-failure" },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"] } },
    { name: "mobile", use: { ...devices["Pixel 7"] }, testMatch: /smoke/ },
  ],
  webServer: {
    command: `rm -f e2e/.e2e.db* && cd .. && uv run ninelives serve examples/shop.py:app --worker --port ${PORT}`,
    url: `http://127.0.0.1:${PORT}/api/health`,
    reuseExistingServer: false,
    timeout: 120_000,
    env: {
      NINELIVES_DB: "sqlite:///dashboard/e2e/.e2e.db",
      SHOP_FLAKINESS: "0.5",
      SHOP_FOLLOW_UP_DELAY: "1.5",
    },
  },
});
