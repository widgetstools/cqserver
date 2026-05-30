# cqclient-go

A Go client SDK for **CQServer**, a pub/sub database. It is a faithful port of
the reference Python SDK.

## Wire protocol

Plain TCP carrying length-prefixed JSON frames: each frame is a big-endian
`uint32` length followed by a JSON body. The top bit of the length
(`0x80000000`) flags a zstd-compressed body; this SDK advertises no compression,
so it is never set and any inbound frame with it set is a protocol error.
Max frame size is 16 MiB.

## Usage

```go
c, _ := cqclient.Connect("127.0.0.1", 9099, 5*time.Second)
defer c.Close()

c.Logon("", "", "", "")                                  // anonymous handshake
c.Publish("/sdk-test", map[string]interface{}{"k": "AAA", "v": 1})

sub, _ := c.SowAndSubscribe("/sdk-test", "", 0)          // snapshot + live deltas
d, _ := sub.NextDelta(time.Second)                       // d.DeltaType == "sow" for snapshot rows

rows, _ := c.Sow("/sdk-test", "")                        // one-shot snapshot
c.Unsubscribe(sub)
```

## Running the example

Start the server:

```
target/release/cqserver --config clients/sdk-smoke.toml
```

Then, from `client-sdks/go`:

```
go run ./example          # optionally: go run ./example <host> <port>
```
