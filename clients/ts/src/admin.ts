import { ClientError } from './types.js';

/** Thin HTTP client for the cqserver admin endpoints. */
export class AdminClient {
  constructor(public readonly host: string, public readonly port: number) {}

  static parse(url: string): AdminClient {
    try {
      const u = new URL(url.startsWith('http') ? url : `http://${url}`);
      const port = u.port ? parseInt(u.port, 10) : 8085;
      return new AdminClient(u.hostname, port);
    } catch {
      throw new ClientError(`invalid admin url: ${url}`);
    }
  }

  async healthz(): Promise<string> {
    return (await this.fetch('/healthz')).text;
  }

  async stats(): Promise<unknown> {
    return JSON.parse((await this.fetch('/stats')).text);
  }

  async topics(): Promise<unknown> {
    return JSON.parse((await this.fetch('/topics')).text);
  }

  async metrics(): Promise<string> {
    return (await this.fetch('/metrics')).text;
  }

  private async fetch(path: string): Promise<{ status: number; text: string }> {
    const url = `http://${this.host}:${this.port}${path}`;
    const resp = await globalThis.fetch(url);
    const text = await resp.text();
    if (!resp.ok) throw new ClientError(`GET ${path}: ${resp.status}`);
    return { status: resp.status, text };
  }
}
