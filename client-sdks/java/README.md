# CQServer Java SDK

A dependency-free Java client (JDK 17+) for **CQServer**, a pub/sub database. It is
a faithful port of the proven Python SDK and uses only `java.net.Socket`,
`java.util.concurrent`, and a small hand-written JSON codec — no external libraries.

## Wire protocol

Plain TCP. Each frame is `[u32 big-endian length][JSON body (UTF-8)]`; the top bit
of the length flags a zstd-compressed body (this SDK advertises none, so it is never
set on send and is rejected on receive). Max frame is 16 MiB.

## Package layout

- `io.cqserver.client.CqClient` — connect / connectUrl / connectAny / logon / publish /
  deltaPublish / sow / sowSql / sowAndSubscribe / subscribe / unsubscribe / heartbeat /
  onClose / close (background reader thread).
- `io.cqserver.client.CqClient.Options` — connection knobs: `connectTimeoutMs`,
  `heartbeatIntervalMs` (fluent setters).
- `io.cqserver.client.Subscription` — `Delta nextDelta(long timeoutMs)` (returns
  `null` on timeout; negative timeout blocks indefinitely), `getLastSequence()`.
- `io.cqserver.client.Delta` — `deltaType`, `subId`, `data`, `sequence`.
- `io.cqserver.client.Json` — minimal JSON parser + serializer.
- `io.cqserver.client.CqException` — runtime exception for server/protocol errors.

## High availability & tuning (parity with the TS / Rust SDKs)

- **Initial-connect failover** — `connectAny(List<String> urls)` tries each
  `tcp://host:port` in a randomised (Fisher-Yates) order and returns the first that
  connects; `getActiveUrl()` reports the winner. Randomisation spreads many
  simultaneously-starting clients across the cluster instead of stampeding URL #0.
- **Automatic heartbeat** — the client sends a `heartbeat` frame every
  `Options.heartbeatIntervalMs` (default 25s; `0` disables) so subscriber-only
  connections survive the server's ~65s idle timeout. `heartbeat()` also sends one
  on demand.
- **Resume after reconnect** — `Subscription.getLastSequence()` tracks the highest
  delivered sequence; on a disconnect (surfaced via `onClose(Runnable)`) reconnect and
  pass `getLastSequence() + 1` as the `bookmark` to a fresh `sowAndSubscribe`. (Live
  reconnect itself is the caller's concern, matching the TS/Rust SDKs.)
- **Client-requested conflation** — `subscribe(topic, filter, conflationMs)` and
  `sowAndSubscribe(topic, filter, bookmark, conflationMs)` ask the server to coalesce
  per-row updates (latest-value) and flush every `conflationMs`. `0` opts out even if
  the topic configures a baseline; `null` uses the topic default.

## Compile & run

Start the server first:

```sh
target/release/cqserver --config clients/sdk-smoke.toml
```

Compile the SDK to a build dir and run the quickstart on the classpath:

```sh
# from the repo root
javac -d build $(find client-sdks/java/src -name '*.java')
javac -d build -cp build client-sdks/java/examples/Quickstart.java
java -cp build Quickstart            # defaults to 127.0.0.1:9099
java -cp build Quickstart 127.0.0.1 9099
```

`Quickstart.java` is in the default package and may also be launched directly in
single-file source mode once the SDK classes are on the classpath:

```sh
java -cp build client-sdks/java/examples/Quickstart.java
```
