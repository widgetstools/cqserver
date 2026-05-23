/// <reference types="node" />
/**
 * Loads pre-generated FI demo JSON files into a running cqserver.
 *
 * Reads:
 *   - examples/data/securities.json
 *   - examples/data/fi-market-data.json
 *   - examples/data/positions.json
 *   - examples/data/trades.json
 *
 * Publishes each row in order, then exits. Use this once at the start of
 * a demo session to seed the server. To get live ticks and continuing
 * fills on top, run `npm run publish-fi-demo` (the live publisher).
 *
 * Tuning (env):
 *   CQ_URL            default tcp://127.0.0.1:9007
 *   DATA_DIR          default ./examples/data
 *   PUB_CONCURRENCY   default 500 (in-flight publishes per chunk)
 *
 * Run with: `npm run load-fi-data`
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { Client } from '../src/index.js';

const URL = process.env.CQ_URL ?? 'tcp://127.0.0.1:9007';
const DATA_DIR =
  process.env.DATA_DIR ?? path.join(process.cwd(), 'examples', 'data');
const PUB_CONCURRENCY = Number(process.env.PUB_CONCURRENCY ?? 500);

interface FileSpec {
  topic: string;
  file: string;
}

const FILES: FileSpec[] = [
  { topic: '/securities', file: 'securities.json' },
  { topic: '/fi-market-data', file: 'fi-market-data.json' },
  // Publish trades and positions in either order — they share keys via
  // book/cusip but are stored in independent topics.
  { topic: '/trades', file: 'trades.json' },
  { topic: '/positions', file: 'positions.json' },
];

function readJson(filename: string): unknown[] {
  const fullPath = path.join(DATA_DIR, filename);
  if (!fs.existsSync(fullPath)) {
    throw new Error(
      `Missing ${fullPath}. Run \`npm run generate-fi-data\` first.`,
    );
  }
  const raw = fs.readFileSync(fullPath, 'utf-8');
  const data = JSON.parse(raw);
  if (!Array.isArray(data)) {
    throw new Error(`${filename} must contain a top-level JSON array`);
  }
  return data;
}

async function publishChunked(
  client: Client,
  topic: string,
  rows: unknown[],
): Promise<void> {
  const start = Date.now();
  for (let i = 0; i < rows.length; i += PUB_CONCURRENCY) {
    const chunk = rows.slice(i, i + PUB_CONCURRENCY);
    await Promise.all(
      chunk.map((row) =>
        client.publish(topic, row as Record<string, unknown>),
      ),
    );
    const done = Math.min(i + chunk.length, rows.length);
    if (done % (PUB_CONCURRENCY * 10) === 0 || done === rows.length) {
      const elapsed = (Date.now() - start) / 1000;
      const rate = done / Math.max(elapsed, 0.001);
      console.log(`  ${topic}: ${done}/${rows.length} (${rate.toFixed(0)} msg/s)`);
    }
  }
}

async function main() {
  console.log(`Connecting to cqserver at ${URL}...`);
  const client = await Client.connect(URL);
  const t0 = Date.now();

  for (const { topic, file } of FILES) {
    const rows = readJson(file);
    console.log(`Loading ${rows.length} rows into ${topic} from ${file}...`);
    await publishChunked(client, topic, rows);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
  console.log(`Loaded in ${elapsed}s.`);
  await client.close();
}

main().catch((err) => {
  console.error('Loader failed:', err);
  process.exit(1);
});
