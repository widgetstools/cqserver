/** Length-prefixed frame transports for the SDK. */

import { ClientError, CqMessage } from './types.js';

export interface Transport {
  send(msg: CqMessage): Promise<void>;
  onFrame(handler: (msg: CqMessage) => void): void;
  close(): Promise<void>;
}

export async function connectTcp(host: string, port: number): Promise<Transport> {
  // Lazy-load 'node:net' so the bundler doesn't choke on browsers.
  const net = await import('node:net');
  return await new Promise<Transport>((resolve, reject) => {
    const socket = net.createConnection({ host, port }, () => {
      resolve(new TcpTransport(socket));
    });
    socket.on('error', reject);
  });
}

export async function connectWs(url: string): Promise<Transport> {
  // Use the global WebSocket — present in browsers and Node 22+.
  // Older Node setups need to inject `ws` as a polyfill before importing.
  const Ctor: typeof WebSocket | undefined =
    (globalThis as Record<string, unknown>).WebSocket as typeof WebSocket | undefined;
  if (!Ctor) {
    throw new ClientError(
      'No WebSocket implementation found. Use Node 22+ or polyfill globalThis.WebSocket.',
    );
  }
  const ws = new Ctor(url);
  ws.binaryType = 'arraybuffer';
  await new Promise<void>((resolve, reject) => {
    ws.addEventListener('open', () => resolve(), { once: true });
    ws.addEventListener('error', () => reject(new ClientError('ws open failed')), {
      once: true,
    });
  });
  return new WsTransport(ws);
}

class TcpTransport implements Transport {
  private buf = Buffer.alloc(0);
  private handlers: Array<(msg: CqMessage) => void> = [];

  constructor(private socket: import('node:net').Socket) {
    socket.on('data', (chunk: Buffer) => {
      this.buf = Buffer.concat([this.buf, chunk]);
      this.drain();
    });
    socket.on('close', () => {
      this.buf = Buffer.alloc(0);
    });
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

class WsTransport implements Transport {
  private handlers: Array<(msg: CqMessage) => void> = [];

  constructor(private ws: WebSocket) {
    this.ws.addEventListener('message', (evt) => {
      const raw = typeof evt.data === 'string' ? evt.data : new TextDecoder().decode(
        evt.data as ArrayBuffer,
      );
      let msg: CqMessage;
      try {
        msg = JSON.parse(raw) as CqMessage;
      } catch {
        return;
      }
      for (const h of this.handlers) h(msg);
    });
  }

  send(msg: CqMessage): Promise<void> {
    this.ws.send(JSON.stringify(msg));
    return Promise.resolve();
  }

  onFrame(handler: (msg: CqMessage) => void) {
    this.handlers.push(handler);
  }

  close(): Promise<void> {
    this.ws.close();
    return Promise.resolve();
  }
}
