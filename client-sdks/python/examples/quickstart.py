"""CQServer Python SDK quickstart.

Run a server first, e.g.:
    target/release/cqserver --config clients/sdk-smoke.toml

Then:
    python3 client-sdks/python/examples/quickstart.py [host] [port]
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cqclient import Client  # noqa: E402

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9099
TOPIC = "/sdk-test"


def main() -> None:
    with Client.connect(HOST, PORT) as c:
        version = c.logon()  # anonymous handshake
        print(f"connected, negotiated protocol version {version}")

        seq = c.publish(TOPIC, {"k": "AAA", "v": 1})
        print(f"published AAA -> seq {seq}")

        # Open a live subscription (snapshot + deltas) before the next write.
        sub = c.sow_and_subscribe(TOPIC)

        # Drain the snapshot (one row so far).
        snap = 0
        while True:
            d = sub.next_delta(timeout=0.5)
            if d is None:
                break
            if d.delta_type == "sow":
                snap += 1
                print(f"  snapshot row: {d.data}")
            else:
                break
        print(f"snapshot delivered {snap} row(s)")

        # A live publish should arrive on the subscription as an add/update.
        c.publish(TOPIC, {"k": "BBB", "v": 2})
        d = sub.next_delta(timeout=2.0)
        print(f"live delta: type={d.delta_type if d else None} data={d.data if d else None}")

        # One-shot SOW shows both rows.
        rows = c.sow(TOPIC)
        print(f"sow returned {len(rows)} rows: {sorted(r.get('k') for r in rows)}")

        c.unsubscribe(sub)


if __name__ == "__main__":
    main()
