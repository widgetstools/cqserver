/**
 * Length-prefixed frame transports for the SDK.
 *
 * This module is the isomorphic entry — it contains the WebSocket
 * transport, which works unchanged in browsers (native `WebSocket`) and
 * Node 22+ (built-in `WebSocket`). The TCP transport lives in
 * `./transport-node.ts` and pulls in `node:net`, so callers that want
 * to stay browser-safe should not import it.
 *
 * `Client.connect(url)` switches between the two based on the URL
 * scheme — for `tcp://`, it dynamically imports the Node module so
 * Vite/Rollup can tree-shake it out of browser builds.
 */

import { ClientError, CqMessage } from './types.js';

export interface Transport {
  send(msg: CqMessage): Promise<void>;
  onFrame(handler: (msg: CqMessage) => void): void;
  /**
   * Fires when the underlying connection terminates for any reason —
   * server-initiated close, network drop, idle timeout. Lets the
   * Client tear down outstanding subscriptions so consumers can
   * detect the disconnect (and reconnect if they want to).
   */
  onClose(handler: () => void): void;
  close(): Promise<void>;
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

class WsTransport implements Transport {
  private handlers: Array<(msg: CqMessage) => void> = [];
  private closeHandlers: Array<() => void> = [];
  private closed = false;

  constructor(private ws: WebSocket) {
    this.ws.addEventListener('message', (evt) => {
      const raw = typeof evt.data === 'string'
        ? evt.data
        : new TextDecoder().decode(evt.data as ArrayBuffer);
      let msg: CqMessage;
      try {
        msg = JSON.parse(raw) as CqMessage;
      } catch {
        return;
      }
      for (const h of this.handlers) h(msg);
    });
    const fireClose = () => {
      if (this.closed) return;
      this.closed = true;
      for (const cb of this.closeHandlers) cb();
    };
    this.ws.addEventListener('close', fireClose);
    this.ws.addEventListener('error', fireClose);
  }

  send(msg: CqMessage): Promise<void> {
    if (this.closed || this.ws.readyState !== this.ws.OPEN) {
      return Promise.reject(new ClientError('transport closed'));
    }
    this.ws.send(JSON.stringify(msg));
    return Promise.resolve();
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
    this.ws.close();
    return Promise.resolve();
  }
}
