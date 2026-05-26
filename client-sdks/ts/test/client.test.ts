import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, ChildProcess } from 'node:child_process';
import { writeFileSync, existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createConnection } from 'node:net';
import { Client } from '../src/client.js';
import { AdminClient } from '../src/admin.js';

const ROOT = join(__dirname, '..', '..', '..');

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const net = require('node:net');
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

async function waitForOpen(host: string, port: number, deadlineMs = 5000) {
  const end = Date.now() + deadlineMs;
  while (Date.now() < end) {
    const ok = await new Promise<boolean>((resolve) => {
      const s = createConnection({ host, port }, () => {
        s.end();
        resolve(true);
      });
      s.on('error', () => resolve(false));
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`port ${host}:${port} did not open in ${deadlineMs}ms`);
}

let tcpPort: number;
let wsPort: number;
let adminPort: number;
let server: ChildProcess | null = null;

beforeAll(async () => {
  let binary = join(ROOT, 'target', 'release', 'cqserver');
  if (!existsSync(binary)) {
    binary = join(ROOT, 'target', 'debug', 'cqserver');
  }
  if (!existsSync(binary)) {
    throw new Error(`server binary not built at ${binary}`);
  }
  tcpPort = await findFreePort();
  wsPort = await findFreePort();
  adminPort = await findFreePort();
  const dir = mkdtempSync(join(tmpdir(), 'cqsrv-ts-'));
  const cfgDir = join(dir, 'config');
  require('node:fs').mkdirSync(cfgDir, { recursive: true });
  writeFileSync(
    join(cfgDir, 'cqserver.toml'),
    `
tcp_addr = "127.0.0.1:${tcpPort}"
websocket_addr = "127.0.0.1:${wsPort}"
websocket_path = "/cq/json"
admin_addr = "127.0.0.1:${adminPort}"
heartbeat_interval_s = 60
heartbeat_idle_timeout_s = 120

[[topics]]
name = "/market-data"
key = ["symbol"]
persist = false
initial_capacity = 100

[[topics]]
name = "/batch-data"
key = ["symbol"]
persist = false
initial_capacity = 100

[[queues]]
name = "/work"

[txlog]
directory = "${join(dir, 'txlog')}"
`,
  );
  server = spawn(binary, [], {
    cwd: dir,
    env: { ...process.env, RUST_LOG: 'warn' },
    stdio: 'ignore',
  });
  await waitForOpen('127.0.0.1', adminPort);
}, 30_000);

afterAll(async () => {
  if (server) {
    server.kill('SIGTERM');
    await new Promise((r) => setTimeout(r, 200));
    if (!server.killed) server.kill('SIGKILL');
  }
});

describe('Client.connectAny (P15 — HA failover)', () => {
  it('rotates past a dead URL to a live one', async () => {
    // 49001 is reserved by the test runner; assume it's free for our
    // "dead" entry. The second URL is the actual running server.
    const dead = 'tcp://127.0.0.1:1'; // port 1 is reserved → always refused
    const live = `tcp://127.0.0.1:${tcpPort}`;
    const client = await Client.connectAny([dead, live]);
    expect(client.activeUrl).toBe(live);
    await client.close();
  }, 15_000);

  it('throws when every URL fails', async () => {
    await expect(
      Client.connectAny(['tcp://127.0.0.1:1', 'tcp://127.0.0.1:2']),
    ).rejects.toThrow();
  }, 15_000);

  it('rejects an empty URL list', async () => {
    await expect(Client.connectAny([])).rejects.toThrow(/empty url list/);
  });
});

describe('Client.publishBatch (P16 — pipelined batched publish)', () => {
  it('publishes N messages in parallel and returns N sequences', async () => {
    const c = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    // Seed publish so the schema is established.
    await c.publish('/batch-data', { symbol: 'BATCH_SEED', price: 1.0 });

    // Prices kept BELOW 100 so this batch doesn't pollute the SOW
    // snapshot the next test (`price > 100` filter) reads from.
    const msgs = Array.from({ length: 50 }, (_, i) => ({
      symbol: `B${i.toString().padStart(2, '0')}`,
      price: 10 + i,
    }));
    const seqs = await c.publishBatch('/batch-data', msgs);
    expect(seqs.length).toBe(50);
    // Every returned sequence must be a positive integer.
    for (const s of seqs) expect(s).toBeGreaterThan(0);
    // Sequences are monotonic per-topic — assert strictly increasing
    // (sortedness, not necessarily contiguity if other publishes
    // landed between).
    const sorted = [...seqs].sort((a, b) => a - b);
    expect(seqs).toEqual(sorted);

    // SOW must show every published row (plus the seed).
    const rows = await c.sow('/batch-data');
    const syms = new Set(rows.map((r) => r.symbol));
    for (let i = 0; i < 50; i++) {
      expect(syms.has(`B${i.toString().padStart(2, '0')}`)).toBe(true);
    }
    await c.close();
  }, 15_000);

  it('publishBatch with empty list resolves immediately', async () => {
    const c = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    const seqs = await c.publishBatch('/batch-data', []);
    expect(seqs).toEqual([]);
    await c.close();
  });
});

describe('Client TLS scheme (Q5)', () => {
  it('rejects a connection to a non-TLS port', async () => {
    // Connecting `tls://` to the plain-TCP port should fail at the
    // TLS handshake (server speaks framed CqMessage, not TLS). The
    // assertion proves the `tls://` scheme is wired through to
    // `connectTls` — exact error message depends on Node's TLS impl.
    await expect(
      Client.connect(`tls://127.0.0.1:${tcpPort}`),
    ).rejects.toThrow();
  }, 15_000);

  it('rejects an obviously-malformed tls url', async () => {
    await expect(Client.connect('tls://')).rejects.toThrow(/bad tls url/);
    await expect(Client.connect('tls://noport')).rejects.toThrow(/bad tls url/);
  });
});

describe('Client over TCP', () => {
  it('publish, subscribe with filter, receive ADD delta', async () => {
    const c = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    // Seed publish to drive schema discovery.
    const seedSeq = await c.publish('/market-data', { symbol: 'SEED', price: 1.0 });
    expect(seedSeq).toBeGreaterThanOrEqual(1);

    const sub = await c.sowAndSubscribe('/market-data', { filter: 'price > 100' });
    const pubSeq = await c.publish('/market-data', { symbol: 'AAPL', price: 150 });

    const delta = await Promise.race([
      sub.nextDelta(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('delta timeout')), 2000)),
    ]);
    expect(delta).not.toBeNull();
    expect((delta as any).deltaType).toBe('add');
    expect((delta as any).data.symbol).toBe('AAPL');
    expect((delta as any).sequence).toBe(pubSeq);
    expect(sub.lastSequence).toBe(pubSeq);

    const rows = await c.sow('/market-data');
    expect(rows.length).toBe(2);

    await c.unsubscribe(sub.subId);
    await c.close();
  }, 15_000);

  it('queue round-robins between two consumers', async () => {
    const a = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    const b = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    const subA = await a.subscribe('/work');
    const subB = await b.subscribe('/work');
    await new Promise((r) => setTimeout(r, 100));

    const p = await Client.connect(`tcp://127.0.0.1:${tcpPort}`);
    for (let i = 1; i <= 6; i++) {
      await p.publish('/work', { i });
    }

    const seen: number[] = [];
    for (let k = 0; k < 3; k++) {
      const da = await subA.nextDelta();
      const db = await subB.nextDelta();
      seen.push((da!.data as any).i, (db!.data as any).i);
    }
    seen.sort((x, y) => x - y);
    expect(seen).toEqual([1, 2, 3, 4, 5, 6]);

    await a.close();
    await b.close();
    await p.close();
  }, 15_000);
});

describe('Admin', () => {
  it('healthz returns ok', async () => {
    const admin = new AdminClient('127.0.0.1', adminPort);
    const txt = await admin.healthz();
    expect(txt.trim()).toBe('ok');
  });

  it('topics lists the configured topics', async () => {
    const admin = new AdminClient('127.0.0.1', adminPort);
    const ts = (await admin.topics()) as Array<{ name: string }>;
    expect(ts.some((t) => t.name === '/market-data')).toBe(true);
  });
});
