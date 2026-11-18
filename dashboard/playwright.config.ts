import { defineConfig, devices } from "@playwright/test";

const PORT = 8766;
const AUTH_PORT = 8767;
const env = { SHOP_FLAKINESS: "0.5", SHOP_FOLLOW_UP_DELAY: "1.5" };

// Two real servers (API + dashboard + an in-process worker), each on a throwaway SQLite file:
// an open one for most tests, and one that requires an API token for the sign-in tests.
export default defineConfig({
  testDir: "e2e",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  use: { baseURL: `http://127.0.0.1:${PORT}`, trace: "retain-on-failure" },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"] }, testIgnore: /auth/ },
    { name: "mobile", use: { ...devices["Pixel 7"] }, testMatch: /smoke/ },
    {
      name: "auth",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${AUTH_PORT}` },
      testMatch: /auth/,
    },
  ],
  webServer: [
    {
      command: `rm -f e2e/.e2e.db* && cd .. && uv run ninelives serve examples/shop.py:app --worker --port ${PORT}`,
      url: `http://127.0.0.1:${PORT}/api/health`,
      reuseExistingServer: false,
      timeout: 120_000,
      env: { ...env, NINELIVES_DB: "sqlite:///dashboard/e2e/.e2e.db" },
    },
    {
      command: `rm -f e2e/.auth.db* && cd .. && uv run ninelives serve examples/shop.py:app --worker --port ${AUTH_PORT} --token e2e-secret`,
      url: `http://127.0.0.1:${AUTH_PORT}/api/health`,
      reuseExistingServer: false,
      timeout: 120_000,
      env: { ...env, NINELIVES_DB: "sqlite:///dashboard/e2e/.auth.db" },
    },
  ],
});
