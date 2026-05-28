/// <reference lib="webworker" />
/**
 * Dedicated-Worker fallback entry. Used when the browser doesn't
 * expose `SharedWorker` (Safari before its 2026 release, mobile WKWebView).
 * Loses the cross-tab connection-sharing benefit; preserves every
 * other Phase 2 win — off-main JSON parse, chunked SOW, coalesced
 * deltas, supervised reconnect.
 */
import { hub, type HubPort } from './hub';

const scope = self as unknown as DedicatedWorkerGlobalScope;

const hubPort: HubPort = {
  postMessage: (msg) => scope.postMessage(msg),
  addEventListener: (type, cb) => scope.addEventListener(type, cb as EventListener),
};
hub.attachPort(hubPort);
