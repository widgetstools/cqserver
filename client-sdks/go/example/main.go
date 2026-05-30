// Command example mirrors the Python SDK quickstart.
//
// Run a server first, e.g.:
//
//	target/release/cqserver --config clients/sdk-smoke.toml
//
// Then:
//
//	go run ./example [host] [port]
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	cqclient "github.com/cqserver/cqclient-go"
)

const topic = "/sdk-test"

func main() {
	host := "127.0.0.1"
	port := 9099
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		p, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid port: %v\n", err)
			os.Exit(1)
		}
		port = p
	}

	c, err := cqclient.Connect(host, port, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	version, err := c.Logon("", "", "", "") // anonymous handshake
	if err != nil {
		fmt.Fprintf(os.Stderr, "logon failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("connected, negotiated protocol version %d\n", version)

	seq, err := c.Publish(topic, map[string]interface{}{"k": "AAA", "v": 1})
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("published AAA -> seq %d\n", seq)

	// Open a live subscription (snapshot + deltas) before the next write.
	sub, err := c.SowAndSubscribe(topic, "", 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sow_and_subscribe failed: %v\n", err)
		os.Exit(1)
	}

	// Drain the snapshot (one row so far).
	snap := 0
	for {
		d, err := sub.NextDelta(500 * time.Millisecond)
		if err != nil {
			fmt.Fprintf(os.Stderr, "next_delta failed: %v\n", err)
			os.Exit(1)
		}
		if d == nil {
			break
		}
		if d.DeltaType == "sow" {
			snap++
			fmt.Printf("  snapshot row: %v\n", d.Data)
		} else {
			break
		}
	}
	fmt.Printf("snapshot delivered %d row(s)\n", snap)

	// A live publish should arrive on the subscription as an add/update.
	if _, err := c.Publish(topic, map[string]interface{}{"k": "BBB", "v": 2}); err != nil {
		fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
		os.Exit(1)
	}
	d, err := sub.NextDelta(2 * time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "next_delta failed: %v\n", err)
		os.Exit(1)
	}
	if d != nil {
		fmt.Printf("live delta: type=%s data=%v\n", d.DeltaType, d.Data)
	} else {
		fmt.Printf("live delta: type=<nil> data=<nil>\n")
	}

	// One-shot SOW shows both rows.
	rows, err := c.Sow(topic, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "sow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("sow returned %d rows: %v\n", len(rows), cqclient.SortedStringValues(rows, "k"))

	if err := c.Unsubscribe(sub); err != nil {
		fmt.Fprintf(os.Stderr, "unsubscribe failed: %v\n", err)
		os.Exit(1)
	}
}
