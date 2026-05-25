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
