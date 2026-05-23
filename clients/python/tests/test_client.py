"""Integration tests: spin up the real cqserver binary and exercise the
Python client against it."""

import asyncio
import os
import pathlib
import shutil
import socket
import subprocess
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]


def _find_free_ports(n: int) -> list[int]:
    socks = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    binary = ROOT / "target" / "release" / "cqserver"
    if not binary.exists():
        binary = ROOT / "target" / "debug" / "cqserver"
    if not binary.exists():
        pytest.skip(f"server binary not built at {binary}")

    workdir = tmp_path_factory.mktemp("cqsrv")
    cfg_dir = workdir / "config"
    cfg_dir.mkdir()
    tcp_port, ws_port, admin_port = _find_free_ports(3)
    cfg = cfg_dir / "cqserver.toml"
    cfg.write_text(
        f"""
tcp_addr = "127.0.0.1:{tcp_port}"
websocket_addr = "127.0.0.1:{ws_port}"
websocket_path = "/cq/json"
admin_addr = "127.0.0.1:{admin_port}"
heartbeat_interval_s = 60
heartbeat_idle_timeout_s = 120

[[topics]]
name = "/market-data"
key = ["symbol"]
persist = false
initial_capacity = 100

[[queues]]
name = "/work"

[txlog]
directory = "{(workdir / 'txlog').as_posix()}"
"""
    )
    proc = subprocess.Popen(
        [str(binary)],
        cwd=workdir,
        env={**os.environ, "RUST_LOG": "warn"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait for the admin endpoint to come up.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", admin_port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)
    else:
        proc.kill()
        pytest.fail("server didn't start in time")

    yield {"tcp": tcp_port, "ws": ws_port, "admin": admin_port}
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.mark.asyncio
async def test_publish_and_subscribe_roundtrip(server):
    from cqclient import Client

    client = await Client.connect(f"tcp://127.0.0.1:{server['tcp']}")

    # Seed publish to drive schema discovery.
    seed_seq = await client.publish(
        "/market-data", {"symbol": "SEED", "price": 1.0}
    )
    assert seed_seq >= 1

    sub = await client.sow_and_subscribe(
        "/market-data", filter="price > 100"
    )

    pub_seq = await client.publish(
        "/market-data", {"symbol": "AAPL", "price": 150.0}
    )
    delta = await asyncio.wait_for(sub.next_delta(), timeout=2.0)
    assert delta is not None
    assert delta.delta_type == "add"
    assert delta.data["symbol"] == "AAPL"
    assert delta.sequence == pub_seq
    assert sub.last_sequence() == pub_seq

    rows = await client.sow("/market-data")
    syms = sorted(r.get("symbol") for r in rows)
    assert syms == ["AAPL", "SEED"]

    await client.unsubscribe(sub.sub_id)
    await client.close()


@pytest.mark.asyncio
async def test_admin_endpoints(server):
    from cqclient import AdminClient

    admin = AdminClient("127.0.0.1", server["admin"])
    assert (await admin.healthz()).strip() == "ok"
    stats = await admin.stats()
    assert "topics" in stats
    topics = await admin.topics()
    names = {t.get("name") for t in topics}
    assert "/market-data" in names


@pytest.mark.asyncio
async def test_queue_round_robin(server):
    from cqclient import Client

    a = await Client.connect(f"tcp://127.0.0.1:{server['tcp']}")
    b = await Client.connect(f"tcp://127.0.0.1:{server['tcp']}")
    sub_a = await a.subscribe("/work")
    sub_b = await b.subscribe("/work")

    # Give the server a moment to register both consumers.
    await asyncio.sleep(0.05)

    producer = await Client.connect(f"tcp://127.0.0.1:{server['tcp']}")
    for i in range(1, 7):
        await producer.publish("/work", {"i": i})

    seen = []
    for _ in range(3):
        seen.append((await asyncio.wait_for(sub_a.next_delta(), 2.0)).data["i"])
        seen.append((await asyncio.wait_for(sub_b.next_delta(), 2.0)).data["i"])
    seen.sort()
    assert seen == [1, 2, 3, 4, 5, 6]

    await a.close()
    await b.close()
    await producer.close()
