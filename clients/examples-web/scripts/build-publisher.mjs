#!/usr/bin/env node
/**
 * Bundle the Atlas publisher into a single Node-runnable .mjs.
 *
 * We use esbuild (already a transitive dependency via Vite) to inline
 * data-gen, refdata, the typed cqserver client, and the rng helper so
 * the publisher can run with plain `node` without any module-resolution
 * gymnastics for `.ts` extensions or @cqserver/client paths.
 */
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..', '..', '..');
const out = join(root, '.demo-run', 'atlas-publisher.mjs');

await build({
  entryPoints: [join(here, 'publisher.ts')],
  outfile: out,
  bundle: true,
  format: 'esm',
  platform: 'node',
  target: 'node20',
  // The cqserver TS client uses dynamic imports for 'node:net' and
  // 'ws', mark them external so they resolve at runtime.
  external: ['node:net', 'ws'],
  // Aliases @cqserver/client → freshly built JS so the publisher uses
  // the same wire codec as the browser store.
  alias: {
    '@cqserver/client': join(root, 'client-sdks/ts/dist/index.js'),
  },
  logLevel: 'info',
});

console.log(`built ${out}`);
