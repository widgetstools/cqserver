/// <reference lib="webworker" />
/**
 * SharedWorker entry — wires each connecting port into the singleton
 * `hub`. The hub does the heavy lifting; this file only adapts the
 * SharedWorker `onconnect` event shape into a `HubPort`.
 */
import { hub, type HubPort } from './hub';

const scope = self as unknown as SharedWorkerGlobalScope;

scope.onconnect = (event: MessageEvent) => {
  const port = event.ports[0];
  if (!port) return;
  const hubPort: HubPort = {
    postMessage: (msg) => port.postMessage(msg),
    addEventListener: (type, cb) => port.addEventListener(type, cb as EventListener),
    start: () => port.start(),
  };
  hub.attachPort(hubPort);
};
