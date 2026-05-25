import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';

// Production: the cqserver mounts the SPA under /ui/* via tower-http
// ServeDir, so assets must resolve to /ui/assets/... not /assets/...
// Dev (vite serve on :5174): the SPA is at the root, base stays /.
export default defineConfig(({ command }) => ({
  plugins: [react(), tailwindcss()],
  base: command === 'build' ? '/ui/' : '/',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    // AG-Grid is large and only used on two pages — split it into
    // its own named chunk so the first-load bundle stays small.
    // Recharts gets a similar treatment.
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes('node_modules/ag-grid')) return 'ag-grid';
          if (id.includes('node_modules/recharts')) return 'recharts';
        },
      },
    },
    // We've split deliberately; raise the warn floor so the build
    // log stays useful for unexpected bloat instead of crying wolf
    // about AG-Grid + recharts which we already know about.
    chunkSizeWarningLimit: 700,
  },
  server: {
    port: 5174,
    strictPort: false,
    proxy: {
      '/admin-api': {
        target: 'http://127.0.0.1:8085',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/admin-api/, ''),
      },
    },
  },
}));
