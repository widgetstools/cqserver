/**
 * Node-only TCP transport.
 *
 * Kept in a separate module so browser bundles never see `node:net` or
 * `Buffer`. `Client.connect()` dynamically imports this file only when
 * the URL scheme is `tcp://`.
 */

import { Buffer } from 'node:buffer';
import type { CqMessage } from './types.js';
import type { Transport } from './transport.js';

export async function connectTcp(host: string, port: number): Promise<Transport> {
  const net = await import('node:net');
  return await new Promise<Transport>((resolve, reject) => {
    const socket = net.createConnection({ host, port }, () => {
      resolve(new TcpTransport(socket));
    });
    socket.on('error', reject);
  });
}

/**
 * Q5 — TLS-wrapped TCP transport. Same length-prefixed frame
 * protocol as plain TCP; just runs over a TLS socket so traffic is
 * encrypted in transit. Selected via `Client.connect("tls://...")`.
 *
 * Options:
 * - `servername` — SNI value the client presents (defaults to `host`).
 *   The server's cert is validated against this name.
 * - `ca` — extra trust-root PEM(s) for self-signed dev certs. When
 *   set, supplements the system trust store.
 * - `rejectUnauthorized` — set to `false` to skip cert validation
 *   entirely (DEV ONLY; never use in production).
 */
export interface TlsConnectOptions {
  servername?: string;
  ca?: string | Buffer | Array<string | Buffer>;
  rejectUnauthorized?: boolean;
}

export async function connectTls(
  host: string,
  port: number,
  opts: TlsConnectOptions = {},
): Promise<Transport> {
  const tls = await import('node:tls');
  return await new Promise<Transport>((resolve, reject) => {
    const socket = tls.connect(
      {
        host,
        port,
        servername: opts.servername ?? host,
        ca: opts.ca,
        rejectUnauthorized: opts.rejectUnauthorized ?? true,
      },
      () => {
        resolve(new TcpTransport(socket));
      },
    );
    socket.on('error', reject);
  });
}

class TcpTransport implements Transport {
  private buf = Buffer.alloc(0);
  private handlers: Array<(msg: CqMessage) => void> = [];
  private closeHandlers: Array<() => void> = [];
  private closed = false;

  constructor(private socket: import('node:net').Socket) {
    socket.on('data', (chunk: Buffer) => {
      this.buf = Buffer.concat([this.buf, chunk]);
      this.drain();
    });
    const fireClose = () => {
      if (this.closed) return;
      this.closed = true;
      this.buf = Buffer.alloc(0);
      for (const cb of this.closeHandlers) cb();
    };
    socket.on('close', fireClose);
    socket.on('end', fireClose);
    socket.on('error', fireClose);
  }

  send(msg: CqMessage): Promise<void> {
    const body = Buffer.from(JSON.stringify(msg), 'utf-8');
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    return new Promise<void>((resolve, reject) => {
      this.socket.write(Buffer.concat([header, body]), (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  onFrame(handler: (msg: CqMessage) => void) {
    this.handlers.push(handler);
  }

  onClose(handler: () => void) {
    if (this.closed) {
      handler();
      return;
    }
    this.closeHandlers.push(handler);
  }

  close(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.socket.end(() => resolve());
    });
  }

  private drain() {
    while (this.buf.length >= 4) {
      const len = this.buf.readUInt32BE(0);
      if (this.buf.length < 4 + len) break;
      const body = this.buf.subarray(4, 4 + len);
      this.buf = this.buf.subarray(4 + len);
      let msg: CqMessage;
      try {
        msg = JSON.parse(body.toString('utf-8')) as CqMessage;
      } catch {
        continue;
      }
      for (const h of this.handlers) h(msg);
    }
  }
}
