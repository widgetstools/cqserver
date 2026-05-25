import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';

// The examples app is intended for local dev + standalone preview. It
// doesn't need to be served from cqserver itself — operators run it
// next to a real cqserver process to explore patterns. We default to
// port :5175 so it can coexist with the admin UI on :5174.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(__dirname, './src') },
  },
  build: {
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes('node_modules/ag-grid')) return 'ag-grid';
          if (id.includes('node_modules/recharts')) return 'recharts';
          if (id.includes('node_modules/dockview')) return 'dockview';
          if (id.includes('node_modules/@codemirror') || id.includes('node_modules/@uiw/react-codemirror')) return 'codemirror';
        },
      },
    },
  },
  server: {
    port: 5175,
    strictPort: false,
  },
});
