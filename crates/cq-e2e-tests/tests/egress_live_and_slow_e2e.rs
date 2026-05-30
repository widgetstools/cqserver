//! Egress under load — live delta fan-out, and slow-consumer isolation.
//!
//! Two scenarios that complement the bulk-SOW `large_egress_e2e`:
//!
//!   1. **High-rate live delta egress.** N fast subscribers, a publisher
//!      floods M live deltas; every subscriber must receive all M (no
//!      drops). Reports fan-out throughput.
//!
//!   2. **Slow consumer at scale.** Under a heavy flood, a never-reading
//!      consumer overflows its disk spillover and is DISCONNECTED (AMPS
//!      parity), while fast subscribers on the same topic keep up and
//!      receive the whole flood — proving one slow consumer can't starve
//!      the others or wedge the server.

use std::time::{Duration, Instant};

use cq_client::{Client, DeltaKind, Subscription};
use cq_e2e_tests::{start_server_with, ServerHandle, ServerOpts, SpilloverOpts, TopicSpec};
use serde_json::{json, Map, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const NVAL: usize = 8;

fn topic(name: &str) -> TopicSpec {
    let mut cols: Vec<(String, String)> = vec![("k".into(), "string".into())];
    for v in 0..NVAL {
        cols.push((format!("v{v}"), "double".into()));
    }
    TopicSpec::new(name, "k").with_inline_columns(cols)
}

fn row(i: usize) -> Value {
    let mut m = Map::new();
    m.insert("k".into(), json!(format!("k{i:08}")));
    for v in 0..NVAL {
        m.insert(format!("v{v}"), json!(i as f64 * 1.0001 + v as f64));
    }
    Value::Object(m)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Flood `n` distinct-key rows (each a live Add) in batches. Returns the
/// publish duration.
async fn flood(c: &Client, topic: &str, n: usize, batch: usize) -> Duration {
    let t = Instant::now();
    let mut i = 0usize;
    while i < n {
        let end = (i + batch).min(n);
        let rows: Vec<Value> = (i..end).map(row).collect();
        c.publish_batch(topic, rows).await.expect("publish_batch");
        i = end;
    }
    t.elapsed()
}

/// Connect with a few retries — at thousands of subscribers a burst of
/// simultaneous SYNs can exceed the OS listen backlog and get refused/reset;
/// a short backoff lets the accept loop drain.
async fn connect_retry(url: &str) -> Client {
    let mut last = None;
    for attempt in 0..6u32 {
        match Client::connect(url).await {
            Ok(c) => return c,
            Err(e) => {
                last = Some(format!("{e:?}"));
                tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt + 1))).await;
            }
        }
    }
    panic!("connect failed after retries: {last:?}");
}

/// Connect + `subscribe` (live-only) `n` subscribers, established in paced
/// parallel waves so thousands of connections set up in a few seconds without
/// overflowing the accept backlog (the existing 10k stress test paces the
/// same way).
async fn establish_subscribers(url: &str, topic: &str, n: usize) -> Vec<(Client, Subscription)> {
    const WAVE: usize = 200;
    let mut out = Vec::with_capacity(n);
    let mut done = 0usize;
    while done < n {
        let this = (n - done).min(WAVE);
        let mut handles = Vec::with_capacity(this);
        for _ in 0..this {
            let url = url.to_string();
            let topic = topic.to_string();
            handles.push(tokio::spawn(async move {
                let c = connect_retry(&url).await;
                let sub = c.subscribe(&topic, None).await.expect("subscribe");
                (c, sub)
            }));
        }
        for h in handles {
            out.push(h.await.expect("establish"));
        }
        done += this;
        // Let the server's accept loop breathe between waves.
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    out
}

/// Drain live (non-snapshot) deltas until `target` are seen or `quiet`
/// elapses without one. Returns how many live deltas arrived.
async fn drain_live(sub: &mut Subscription, target: usize, quiet: Duration) -> usize {
    let mut got = 0usize;
    loop {
        match tokio::time::timeout(quiet, sub.next_delta()).await {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => continue,
            Ok(Some(_)) => {
                got += 1;
                if got >= target {
                    return got;
                }
            }
            Ok(None) | Err(_) => return got,
        }
    }
}

// ───────────────────────── 1. live delta egress ─────────────────────────

// 8 worker threads + parallel connection setup so this scales to thousands
// of subscribers. To run at high connection counts, shrink the per-connection
// payload and run release, e.g.:
//   CQ_DELTA_SUBS=5000 CQ_DELTA_N=200 cargo test --release -p cq-e2e-tests \
//     --test egress_live_and_slow_e2e high_rate -- --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn high_rate_live_delta_egress_reaches_every_subscriber() {
    let deltas_n = env_usize("CQ_DELTA_N", 20_000);
    let subs_n = env_usize("CQ_DELTA_SUBS", 8);

    let server = start_server_with(
        vec![topic("/stream")],
        ServerOpts {
            // Generous queue so fast drainers never overflow under the flood.
            outbound_queue_capacity: 16_384,
            ..ServerOpts::default()
        },
    )
    .await;
    let url = server.tcp_url();

    // Establish all subscribers in parallel (sequential connect+subscribe
    // RPCs don't scale to thousands), then hand each off to a drain task.
    let established = establish_subscribers(&url, "/stream", subs_n).await;
    let mut tasks = Vec::with_capacity(subs_n);
    for (sidx, (c, mut sub)) in established.into_iter().enumerate() {
        tasks.push(tokio::spawn(async move {
            let _hold = c; // keep the connection alive for the task's life
            let got = drain_live(&mut sub, deltas_n, Duration::from_secs(5)).await;
            (sidx, got)
        }));
    }
    // Let every subscribe register before the flood.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pubc = Client::connect(&url).await.expect("connect pub");
    let pub_d = flood(&pubc, "/stream", deltas_n, 200).await;

    let egress_t = Instant::now();
    for t in tasks {
        let (sidx, got) = t.await.expect("join");
        assert_eq!(
            got, deltas_n,
            "subscriber {sidx} got {got}/{deltas_n} live deltas — fan-out dropped some"
        );
    }
    let egress_secs = egress_t.elapsed().as_secs_f64();
    let fanout = deltas_n * subs_n;
    eprintln!(
        "\nLive delta egress: {deltas_n} deltas × {subs_n} subs = {fanout} deliveries\n\
         publish: {:.2}s ({:.0} deltas/s)\n\
         fan-out drained in +{egress_secs:.2}s  (~{:.0} deliveries/s)\n",
        pub_d.as_secs_f64(),
        deltas_n as f64 / pub_d.as_secs_f64().max(1e-6),
        fanout as f64 / (pub_d.as_secs_f64() + egress_secs).max(1e-6),
    );
}

// ───────────────────── 2. slow consumer at scale ─────────────────────

async fn open_silent_subscriber(port: u16, topic: &str) -> TcpStream {
    let mut s = TcpStream::connect(format!("127.0.0.1:{port}")).await.expect("tcp");
    let msg = json!({ "c": "sow_and_subscribe", "cid": "silent", "t": topic });
    let payload = serde_json::to_vec(&msg).unwrap();
    s.write_all(&(payload.len() as u32).to_be_bytes()).await.unwrap();
    s.write_all(&payload).await.unwrap();
    s // deliberately never read
}

async fn metrics(server: &ServerHandle) -> String {
    reqwest::get(format!("{}/metrics", server.admin_url()))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

fn counter(metrics: &str, name: &str) -> u64 {
    let mut total = 0u64;
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        let labelled = format!("{name}{{");
        let plain = format!("{name} ");
        let after = if line.starts_with(&labelled) {
            line.split_once('}').map(|(_, r)| r).unwrap_or("")
        } else if let Some(r) = line.strip_prefix(&plain) {
            r
        } else {
            continue;
        };
        if let Ok(v) = after.split_whitespace().last().unwrap_or("0").parse::<f64>() {
            total += v as u64;
        }
    }
    total
}

async fn topic_sub_count(server: &ServerHandle, topic: &str) -> usize {
    let arr: Vec<Value> = reqwest::get(format!("{}/subscriptions", server.admin_url()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap_or_default();
    arr.iter()
        .filter(|s| s.get("topic").and_then(|v| v.as_str()) == Some(topic))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn slow_consumer_disconnects_under_flood_while_fast_subs_keep_up() {
    let flood_n = env_usize("CQ_FLOOD_N", 40_000);
    let fast_n = env_usize("CQ_FAST_SUBS", 3);

    let server = start_server_with(
        vec![topic("/flood")],
        ServerOpts {
            outbound_queue_capacity: 8_192,
            spillover: Some(SpilloverOpts {
                // Small disk cushion → the never-reading consumer over-caps
                // and is disconnected; fast drainers never touch it.
                max_bytes_per_sub: 256 * 1024,
            }),
            ..ServerOpts::default()
        },
    )
    .await;
    let url = server.tcp_url();

    // Fast subscribers (live-only), established in parallel, draining in tasks.
    let mut fast = Vec::with_capacity(fast_n);
    for (sidx, (c, mut sub)) in establish_subscribers(&url, "/flood", fast_n)
        .await
        .into_iter()
        .enumerate()
    {
        fast.push(tokio::spawn(async move {
            let _hold = c;
            (sidx, drain_live(&mut sub, flood_n, Duration::from_secs(6)).await)
        }));
    }
    // One silent consumer that never reads — its queue + spillover fill.
    let _silent = open_silent_subscriber(server.tcp_port, "/flood").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        topic_sub_count(&server, "/flood").await,
        fast_n + 1,
        "expected all subscribers registered before the flood"
    );

    // Flood from the publisher.
    let pubc = Client::connect(&url).await.expect("connect pub");
    flood(&pubc, "/flood", flood_n, 200).await;

    // The silent consumer must be disconnected on spillover over-cap, and
    // the registry should drop back to just the fast subs.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut silent_cut = false;
    loop {
        let cuts = counter(&metrics(&server).await, "cq_slow_consumer_disconnect_total");
        let live = topic_sub_count(&server, "/flood").await;
        if cuts > 0 && live <= fast_n {
            silent_cut = true;
            break;
        }
        if Instant::now() > deadline {
            panic!("silent consumer not disconnected under flood (cuts={cuts}, live={live})");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(silent_cut);

    // The fast subscribers were unaffected: each received the whole flood.
    for t in fast {
        let (sidx, got) = t.await.expect("join");
        assert_eq!(
            got, flood_n,
            "fast subscriber {sidx} got {got}/{flood_n} — a slow consumer starved it"
        );
    }
}
