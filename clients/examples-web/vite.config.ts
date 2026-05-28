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
    alias: {
      '@': path.resolve(__dirname, './src'),
      // Resolve the cqserver TS SDK to its compiled dist so the browser
      // store and the Node publisher use exactly the same client code.
      // Stops at the WS transport in the browser; the TCP transport
      // (transport-node.js) is only dynamically imported when a tcp://
      // URL is passed to Client.connect, which never happens here.
      '@cqserver/client': path.resolve(__dirname, '../../client-sdks/ts/dist/index.js'),
    },
  },
  build: {
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      // The SDK's `transport-node.js` imports `node:net`, `node:tls`,
      // `node:buffer` at module top-level. It's only reachable via a
      // dynamic import behind `Client.connect("tcp://...")` — never
      // exercised from the browser. Externalise the node: builtins so
      // Rollup stops trying to bundle them; the dynamic import itself
      // is unreachable in browser code paths, so the missing runtime
      // is never invoked.
      external: (id) =>
        id.startsWith('node:') ||
        id === '../../client-sdks/ts/dist/transport-node.js' ||
        id.endsWith('/transport-node.js'),
      output: {
        manualChunks: (id) => {
          if (id.includes('node_modules/ag-grid')) return 'ag-grid';
          if (id.includes('node_modules/recharts')) return 'recharts';
          if (id.includes('node_modules/@widgetstools')) return 'dockmanager';
          if (id.includes('node_modules/@codemirror') || id.includes('node_modules/@uiw/react-codemirror')) return 'codemirror';
        },
      },
    },
  },
  worker: {
    // The cqserver SharedWorker imports `@cqserver/client`, which pulls
    // the same SDK that needs the `transport-node.js` externalisation
    // dance as the main bundle. Worker builds get a separate Rollup pass
    // so they don't inherit `build.rollupOptions` — replicate the
    // externals here so `cq-worker.shared.ts` and `cq-worker.dedicated.ts`
    // compile cleanly.
    format: 'es',
    rollupOptions: {
      external: (id) =>
        id.startsWith('node:') ||
        id === '../../client-sdks/ts/dist/transport-node.js' ||
        id.endsWith('/transport-node.js'),
    },
  },
  server: {
    port: 5175,
    strictPort: false,
    proxy: {
      // The query-builder schema catalog is served by cqserver's admin
      // HTTP port (:8085), a different origin from this dev server
      // (:5175). Proxy it same-origin under /cq-admin so the browser
      // fetch needs no CORS. window.CQ_ADMIN_URL can override the base
      // for standalone deploys (see src/lib/catalog.ts).
      '/cq-admin': {
        target: 'http://127.0.0.1:8085',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/cq-admin/, ''),
      },
    },
  },
});
