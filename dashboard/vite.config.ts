import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The build lands inside the Python package so `ninelives serve` ships the dashboard.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: { outDir: "../ninelives/server/static", emptyOutDir: true },
  server: { proxy: { "/api": "http://127.0.0.1:8000" } },
});
