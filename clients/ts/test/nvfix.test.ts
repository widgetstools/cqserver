import { describe, it, expect } from 'vitest';
import { encode, decode, decodeTyped, NvFixError } from '../src/nvfix.js';

describe('nvfix', () => {
  it('roundtrips string fields', () => {
    const bytes = encode({ a: 'hello', b: 'world' });
    expect(decode(bytes)).toEqual({ a: 'hello', b: 'world' });
  });

  it('decodeTyped recovers numbers and bools', () => {
    const bytes = new TextEncoder().encode('qty=100\x01price=99.5\x01active=true\x01name=Alice\x01');
    expect(decodeTyped(bytes)).toEqual({
      qty: 100,
      price: 99.5,
      active: true,
      name: 'Alice',
    });
  });

  it('rejects illegal field names', () => {
    expect(() => encode({ 'bad=name': 'x' })).toThrow(NvFixError);
  });

  it('rejects nested values', () => {
    expect(() => encode({ k: { nested: 1 } })).toThrow(NvFixError);
  });

  it('null encodes as empty value', () => {
    const bytes = encode({ absent: null });
    expect(new TextDecoder().decode(bytes)).toEqual('absent=\x01');
    expect(decode(bytes)).toEqual({ absent: '' });
  });
});
