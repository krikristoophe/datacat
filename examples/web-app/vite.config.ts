import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
  define: {
    // Expose env vars to the app
    "import.meta.env.VITE_DATACAT_URL": JSON.stringify(
      process.env.VITE_DATACAT_URL ?? "http://127.0.0.1:8090"
    ),
    "import.meta.env.VITE_DEMO_BACKEND_URL": JSON.stringify(
      process.env.VITE_DEMO_BACKEND_URL ?? "http://127.0.0.1:8091"
    ),
  },
});
