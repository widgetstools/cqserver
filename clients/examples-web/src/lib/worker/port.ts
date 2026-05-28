/**
 * Main-thread bridge to the cqserver worker.
 *
 * One SharedWorker per origin when the browser supports it; one
 * dedicated Worker per tab otherwise. Both expose the same typed
 * `WorkerPort` to the React hooks above — `subId`-tagged
 * subscribe/unsubscribe requests and a single `onMessage` fan-out.
 *
 * Vite resolves the worker entry URLs at build time via `?sharedworker`
 * and `?worker`; both produce a constructor that knows where the
 * bundle lives.
 */
import SharedWorkerCtor from './cq-worker.shared.ts?sharedworker';
import DedicatedWorkerCtor from './cq-worker.dedicated.ts?worker';
import type { ClientMsg, ServerMsg } from './protocol';

export type WorkerConnectionStatus = 'connecting' | 'connected' | 'disconnected';

export interface WorkerPort {
  /** Fire-and-forget. The hub responds via `onMessage`. */
  send(msg: ClientMsg): void;
  /** Subscribe to every message from the hub. Returns an unsubscribe fn. */
  onMessage(cb: (msg: ServerMsg) => void): () => void;
  /** Subscribe to top-level connection state. Returns an unsubscribe fn. */
  onConnectionChange(cb: (status: WorkerConnectionStatus) => void): () => void;
  /** Current top-level worker-connection status. */
  getConnectionStatus(): WorkerConnectionStatus;
  /** True if running over a SharedWorker (cross-tab connection sharing). */
  isShared(): boolean;
}

class Port implements WorkerPort {
  private listeners = new Set<(msg: ServerMsg) => void>();
  private connListeners = new Set<(s: WorkerConnectionStatus) => void>();
  private connStatus: WorkerConnectionStatus = 'connecting';
  private shared: boolean;
  private postFn: (msg: ClientMsg) => void;

  constructor(
    shared: boolean,
    postFn: (msg: ClientMsg) => void,
    addMessageListener: (cb: (e: MessageEvent<ServerMsg>) => void) => void,
  ) {
    this.shared = shared;
    this.postFn = postFn;
    addMessageListener((e) => this.onIncoming(e.data));
    // Tell the worker we're here so it returns a `hello-ack`. This also
    // triggers the connection bootstrap if no other port has yet.
    const tabId = `t${Math.random().toString(36).slice(2, 10)}`;
    this.postFn({ kind: 'hello', tabId });
  }

  private setConnStatus(s: WorkerConnectionStatus): void {
    if (this.connStatus === s) return;
    this.connStatus = s;
    for (const cb of this.connListeners) cb(s);
  }

  private onIncoming(msg: ServerMsg): void {
    if (msg.kind === 'connected') this.setConnStatus('connected');
    else if (msg.kind === 'disconnected') this.setConnStatus('disconnected');
    for (const cb of this.listeners) cb(msg);
  }

  send(msg: ClientMsg): void {
    this.postFn(msg);
  }
  onMessage(cb: (msg: ServerMsg) => void): () => void {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }
  onConnectionChange(cb: (s: WorkerConnectionStatus) => void): () => void {
    this.connListeners.add(cb);
    return () => this.connListeners.delete(cb);
  }
  getConnectionStatus(): WorkerConnectionStatus {
    return this.connStatus;
  }
  isShared(): boolean {
    return this.shared;
  }
}

let portSingleton: WorkerPort | null = null;

export function getCqPort(): WorkerPort {
  if (portSingleton) return portSingleton;
  // SharedWorker preferred. Safari WebView etc. expose neither — falling
  // back to a real `Worker` keeps a single tab functional even if we
  // lose cross-tab sharing.
  const hasShared = typeof SharedWorker !== 'undefined';
  if (hasShared) {
    const sw = new SharedWorkerCtor({ name: 'cqserver-hub' });
    sw.port.start();
    portSingleton = new Port(
      true,
      (m) => sw.port.postMessage(m),
      (cb) => sw.port.addEventListener('message', cb as EventListener),
    );
  } else {
    const w = new DedicatedWorkerCtor({ name: 'cqserver-hub' });
    portSingleton = new Port(
      false,
      (m) => w.postMessage(m),
      (cb) => w.addEventListener('message', cb as EventListener),
    );
  }
  return portSingleton;
}
