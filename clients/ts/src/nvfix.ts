/** NVFIX payload codec (name=value\x01...). Mirrors the Rust + Python helpers. */

export class NvFixError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = 'NvFixError';
  }
}

const SOH = 0x01;

export function encode(record: Record<string, unknown>): Uint8Array {
  const out: number[] = [];
  const enc = new TextEncoder();
  for (const [k, v] of Object.entries(record)) {
    if (!k || k.includes('=') || k.includes('\x01')) {
      throw new NvFixError(`illegal field name: ${k}`);
    }
    let sval: string;
    if (v === null || v === undefined) {
      sval = '';
    } else if (typeof v === 'string') {
      if (v.includes('\x01')) throw new NvFixError('value contains SOH');
      sval = v;
    } else if (typeof v === 'number' || typeof v === 'boolean') {
      sval = String(v);
    } else {
      throw new NvFixError('nested values not allowed in NVFIX');
    }
    for (const b of enc.encode(k)) out.push(b);
    out.push(0x3d); // '='
    for (const b of enc.encode(sval)) out.push(b);
    out.push(SOH);
  }
  return new Uint8Array(out);
}

export function decode(bytes: Uint8Array): Record<string, string> {
  const dec = new TextDecoder();
  const result: Record<string, string> = {};
  let start = 0;
  for (let i = 0; i <= bytes.length; i++) {
    if (i === bytes.length || bytes[i] === SOH) {
      if (i === start) {
        start = i + 1;
        continue;
      }
      const field = bytes.subarray(start, i);
      const eq = field.indexOf(0x3d);
      if (eq < 0) throw new NvFixError('field without `=`');
      const name = dec.decode(field.subarray(0, eq));
      const value = dec.decode(field.subarray(eq + 1));
      result[name] = value;
      start = i + 1;
    }
  }
  return result;
}

export function decodeTyped(bytes: Uint8Array): Record<string, unknown> {
  const raw = decode(bytes);
  const out: Record<string, unknown> = {};
  for (const [k, s] of Object.entries(raw)) {
    if (/^-?\d+$/.test(s)) {
      out[k] = parseInt(s, 10);
    } else if (/^-?\d+(\.\d+)?$/.test(s)) {
      out[k] = parseFloat(s);
    } else if (s === 'true') {
      out[k] = true;
    } else if (s === 'false') {
      out[k] = false;
    } else {
      out[k] = s;
    }
  }
  return out;
}
