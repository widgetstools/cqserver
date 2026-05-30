//! Transport-agnostic delta delivery and evaluator loop.
//!
//! The evaluator is a single long-lived thread per topic. It pops
//! `MutationEvent`s from the topic's crossbeam channel, calls
//! `Topic::evaluate_row` to compute deltas, and routes each delta to the
//! owning subscription's outbound queue via the `SessionRegistry`.
//!
//! **Encode-once-fan-out** (JSON path): for every JSON-coded route
//! reached during an evaluator pass we serialize the row body at most
//! once per unique `Arc<row_data>` identity. The body bytes are then
//! reused across:
//!
//! 1. **Direct sends** — non-conflated routes build a per-subscriber
//!    envelope around the shared body and `try_send` it.
//! 2. **Conflator submits** — the pre-encoded bytes ride along on the
//!    `Delta` (`encoded_body_json`). The per-sub flush loop stitches
//!    the envelope at flush time without re-serializing the body.
//!
//! MessagePack routes still encode per frame (less common — most
//! clients use JSON).
//!
//! Three Prometheus counters expose the savings:
//!   - `cq_delta_body_encodes_total` — body serialized (any path)
//!   - `cq_delta_body_reuses_total`  — body reused via the direct cache
//!   - `cq_conflator_body_reuses_total` — body reused at conflator flush

use crate::session::{
    build_json_delta_frame, encode_frame, encode_row_body_json, try_submit_to_conflator,
    SessionRegistry,
};
use cq_core::subscription::{Delta, DeltaType};
use cq_core::topic::{MutationEvent, SharedTopic};
use cq_protocol::message::CqMessage;
use cq_protocol::serialization::Codec;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::sync::mpsc;

/// Capacity for each sharded-evaluator lane channel. Bounded so a lane
/// that falls behind backpressures the dispatcher (and, transitively, the
/// publisher) instead of buffering an unbounded `Arc<MutationEvent>`
/// backlog. The cap is generous so it never blocks under healthy load;
/// the dispatcher's `send` blocks (not drops) when a lane is full, which
/// preserves per-subscription delta ordering and correctness — dropping
/// events would silently desync a subscription forever.
const LANE_CHANNEL_CAP: usize = 65_536;

fn dt_label(dt: DeltaType) -> &'static str {
    match dt {
        DeltaType::Add => "add",
        DeltaType::Update => "update",
        DeltaType::Remove => "remove",
        DeltaType::Oof => "oof",
    }
}

/// Single-delta delivery (no body-cache). Kept for direct callers
/// (tests, queue path, etc.); the evaluator hot loop uses
/// `deliver_delta_cached` so it can amortize encode cost.
pub fn deliver_delta(delta: &Delta, registry: &SessionRegistry) {
    let mut cache: HashMap<usize, Arc<Vec<u8>>> = HashMap::new();
    deliver_delta_cached(delta, registry, &mut cache);
}

/// Deliver one delta, reusing the body encoding from `cache` when the
/// route's codec allows. `cache` is keyed by Arc identity of
/// `delta.row_data` so subscribers sharing the same projection result
/// (i.e., the same Arc) get the body bytes serialized exactly once —
/// **and** if any of those subs use conflation, the pre-encoded bytes
/// ride along on the submitted Delta so its flush loop doesn't have to
/// re-encode either.
pub fn deliver_delta_cached(
    delta: &Delta,
    registry: &SessionRegistry,
    body_cache: &mut HashMap<usize, Arc<Vec<u8>>>,
) {
    let dt_str = dt_label(delta.delta_type);

    let route = match registry.get(delta.subscription_id.as_ref()) {
        Some(r) => r,
        None => return,
    };

    // Pre-encode the body once per unique row_data Arc per pass when the
    // route's codec is JSON. Whether the sub uses direct delivery or
    // conflation, the bytes are reusable.
    let body: Option<Arc<Vec<u8>>> = if matches!(route.codec, Codec::Json) {
        let key = Arc::as_ptr(&delta.row_data) as usize;
        match body_cache.get(&key) {
            Some(b) => {
                metrics::counter!("cq_delta_body_reuses_total").increment(1);
                Some(b.clone())
            }
            None => {
                metrics::counter!("cq_delta_body_encodes_total").increment(1);
                match encode_row_body_json(&delta.row_data) {
                    Some(bytes) => {
                        let arc = Arc::new(bytes);
                        body_cache.insert(key, arc.clone());
                        Some(arc)
                    }
                    None => {
                        tracing::warn!(
                            sub = %delta.subscription_id,
                            "Delta body serialize failed"
                        );
                        return;
                    }
                }
            }
        }
    } else {
        None
    };

    // Conflator owns its own per-sub state. Stamp the pre-encoded body
    // onto the delta so the flush loop can skip re-encoding.
    if route.conflator.is_some() {
        let mut delta_with_body = delta.clone();
        delta_with_body.encoded_body_json = body;
        try_submit_to_conflator(&route, &delta_with_body);
        return;
    }

    let mut frame = if let Some(b) = body {
        match build_json_delta_frame(&delta.subscription_id, dt_str, delta.sequence, &b) {
            Some(f) => f,
            None => {
                tracing::warn!(sub = %delta.subscription_id, "Delta envelope build failed");
                return;
            }
        }
    } else {
        // MessagePack: fall back to the per-frame encode for now. A
        // future optimization could cache a tagged MessagePack value
        // similarly, but the demo + most clients use JSON.
        let mut msg =
            CqMessage::delta(&delta.subscription_id, dt_str, (*delta.row_data).clone());
        msg.sequence = Some(delta.sequence);
        match encode_frame(route.codec, &msg) {
            Some(f) => {
                metrics::counter!("cq_delta_body_encodes_total").increment(1);
                f
            }
            None => {
                tracing::warn!(sub = %delta.subscription_id, "Delta serialize failed");
                return;
            }
        }
    };

    // S21: route through spillover when the route has one attached
    // AND either (a) there's already a backlog (preserve order) or
    // (b) the queue is full. Without (a), live frames could skip
    // ahead of backlogged ones and reach the consumer out of order.
    let must_spill = route
        .spillover
        .as_ref()
        .is_some_and(|sp| !sp.is_empty());
    if !must_spill {
        // Move the frame into the channel; on `Full` the error hands it
        // back so we can fall through to spillover without ever cloning
        // on the common (delivered) path.
        match route.tx.try_send(frame) {
            Ok(()) => {
                metrics::counter!("cq_deltas_delivered_total").increment(1);
                // Update server-side bookmark store so MOST_RECENT
                // resumption works across reconnects. Only meaningful
                // when both client_name and store are set on the route.
                if let (Some(cname), Some(store)) =
                    (route.client_name.as_deref(), route.bookmark_store.as_ref())
                {
                    crate::router::record_bookmark(
                        store,
                        cname,
                        &route.topic,
                        delta.sequence,
                    );
                }
                route
                    .last_seq
                    .fetch_max(delta.sequence, Ordering::Relaxed);
                return;
            }
            Err(mpsc::error::TrySendError::Full(returned)) => {
                // Recover ownership and fall through to spillover handling.
                frame = returned;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!(
                    "cq_deltas_dropped_total",
                    "reason" => "receiver_closed"
                )
                .increment(1);
                tracing::debug!(
                    sub = %delta.subscription_id,
                    "Delta receiver closed"
                );
                return;
            }
        }
    }
    // Capacity exceeded. AMPS parity: a subscriber that can't keep up is
    // offlined to disk (spillover, the AMPS disk cushion) if configured, and
    // DISCONNECTED when even that overflows its cap — never silently dropped
    // past the cushion (AMPS disconnects on `MessageDiskLimit`, never leaving
    // the client with a quietly incomplete view).
    //
    // Without spillover configured there is no disk cushion, so this falls
    // back to the legacy best-effort drop (+ degraded-route flag for the
    // slow-consumer watcher). For full AMPS slow-consumer parity, configure
    // `[transport.spillover]`.
    if let Some(sp) = route.spillover.as_ref() {
        match sp.write_frame(&frame) {
            Ok(()) => {
                metrics::counter!("cq_spillover_writes_total").increment(1);
            }
            Err(crate::spillover::SpilloverError::OverCap { .. }) => {
                disconnect_slow_consumer(&route, delta, "spillover_over_cap");
            }
            Err(e) => {
                tracing::warn!(
                    sub = %delta.subscription_id,
                    error = %e,
                    "Spillover write failed"
                );
                disconnect_slow_consumer(&route, delta, "spillover_io");
            }
        }
    } else {
        route.dropped.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("cq_deltas_dropped_total", "reason" => "queue_full")
            .increment(1);
        tracing::warn!(
            sub = %delta.subscription_id,
            topic = %route.topic,
            "Delta queue full; dropping (consumer can't keep up — configure \
             [transport.spillover] for AMPS-style offline-then-disconnect)"
        );
        mark_state_loss_if_removing(&route, delta);
    }
}

/// AMPS-parity slow-consumer disconnect: a subscriber's outbound capacity
/// was exhausted (queue full with no spillover, or spillover over its disk
/// cap). Signal the owning connection to close rather than silently dropping
/// the frame — AMPS never leaves a subscriber with an incomplete view; it
/// disconnects the client, which then reconnects and re-SOWs.
fn disconnect_slow_consumer(
    route: &crate::session::DeliveryRoute,
    delta: &Delta,
    reason: &'static str,
) {
    route.dropped.fetch_add(1, Ordering::Relaxed);
    metrics::counter!("cq_slow_consumer_disconnect_total", "reason" => reason).increment(1);
    tracing::warn!(
        sub = %delta.subscription_id,
        topic = %route.topic,
        session = %route.session_id,
        reason,
        "Slow consumer exceeded outbound capacity; disconnecting connection (AMPS parity)"
    );
    route.request_disconnect();
}

/// Flag a route degraded when a Remove/OOF delta was dropped on the legacy
/// (no-spillover) drop path. Add/Update drops self-correct; a lost
/// Remove/OOF leaves the client with a phantom row, so the slow-consumer
/// watcher force-resyncs degraded routes. (The spillover path disconnects
/// instead of dropping, so this only fires when no spillover is configured.)
fn mark_state_loss_if_removing(route: &crate::session::DeliveryRoute, delta: &Delta) {
    if !matches!(delta.delta_type, DeltaType::Remove | DeltaType::Oof) {
        return;
    }
    if !route.is_degraded() {
        route.mark_degraded();
        metrics::counter!(
            "cq_subscription_state_loss_total",
            "topic" => route.topic.clone()
        )
        .increment(1);
        tracing::error!(
            sub = %delta.subscription_id,
            topic = %route.topic,
            delta_type = dt_label(delta.delta_type),
            "Dropped a Remove/OOF delta (queue full, no spillover); \
             subscription degraded — client must resync"
        );
    }
}

/// Spawn the per-topic evaluator thread. Blocks on the mutation channel;
/// exits when the topic's `Sender` is dropped (i.e., the topic is gone).
pub fn spawn_evaluator(
    topic: SharedTopic,
    rx: Receiver<MutationEvent>,
    registry: SessionRegistry,
) -> std::io::Result<JoinHandle<()>> {
    let topic_name = topic.name().to_string();
    thread::Builder::new()
        .name(format!("evaluator:{}", topic_name))
        .spawn(move || {
            tracing::info!(topic = %topic_name, "Evaluator thread started");
            // Body-encode cache: one slot per evaluator pass. The cache
            // lives outside the inner loop so the allocations are
            // amortized across mutation events.
            let mut body_cache: HashMap<usize, Arc<Vec<u8>>> = HashMap::new();
            for event in rx.iter() {
                // S46 / review C1: route through the predicate index so a
                // sparse publish only re-evaluates subscriptions that
                // reference a changed column. `changed_cols == None`
                // (full-row publishes, deletes, recovery replay) still
                // falls through to the all-subs path inside
                // `evaluate_row_kind_indexed`, so this is a strict
                // superset of the previous behavior.
                let deltas = topic.evaluate_row_kind_indexed(
                    event.row,
                    event.sequence,
                    event.kind,
                    event.changed_cols.as_deref(),
                );
                body_cache.clear();
                for delta in &deltas {
                    deliver_delta_cached(delta, &registry, &mut body_cache);
                }
            }
            tracing::info!(topic = %topic_name, "Evaluator thread exiting");
        })
}

/// Spawn evaluator thread(s) for a topic, honoring its configured lane
/// count. `eval_lanes() <= 1` keeps the historical single-thread loop
/// (zero overhead); `> 1` spins up the sharded form. Returns one
/// `JoinHandle` per spawned thread (1 for single-lane, `lanes + 1` for
/// the sharded form: N lane workers + 1 dispatcher).
pub fn spawn_evaluators(
    topic: SharedTopic,
    rx: Receiver<MutationEvent>,
    registry: SessionRegistry,
) -> std::io::Result<Vec<JoinHandle<()>>> {
    let lanes = topic.eval_lanes();
    if lanes <= 1 {
        Ok(vec![spawn_evaluator(topic, rx, registry)?])
    } else {
        spawn_sharded_evaluator(topic, rx, registry, lanes)
    }
}

/// Multi-lane evaluator. A single dispatcher thread drains the topic's
/// mutation channel and broadcasts each event (wrapped in an `Arc` so the
/// fanout is N pointer clones, not N `changed_cols` Vec clones) to every
/// lane's channel **in topic-sequence order**. One evaluator thread per
/// lane then evaluates only the subscriptions pinned to that lane (see
/// `Topic::engine_for`).
///
/// Per-subscription delta ordering is preserved: a subscription lives on
/// exactly one lane, events reach each lane in order (single producer →
/// FIFO crossbeam channels), and the lane processes them serially — so a
/// subscription's conflation state is only ever mutated by one thread, in
/// sequence order. We need an explicit dispatcher because crossbeam's
/// `Receiver` is work-*stealing* (MPMC): cloning it into N lanes would
/// hand each event to only one lane, not broadcast it.
///
/// Returns N lane handles followed by the dispatcher handle.
pub fn spawn_sharded_evaluator(
    topic: SharedTopic,
    rx: Receiver<MutationEvent>,
    registry: SessionRegistry,
    lanes: usize,
) -> std::io::Result<Vec<JoinHandle<()>>> {
    let topic_name = topic.name().to_string();
    let mut lane_txs = Vec::with_capacity(lanes);
    let mut handles = Vec::with_capacity(lanes + 1);

    for lane in 0..lanes {
        let (tx, lane_rx) = crossbeam_channel::bounded::<Arc<MutationEvent>>(LANE_CHANNEL_CAP);
        lane_txs.push(tx);
        let topic = topic.clone();
        let registry = registry.clone();
        let tname = topic_name.clone();
        let handle = thread::Builder::new()
            .name(format!("evaluator:{}:{}", tname, lane))
            .spawn(move || {
                // Per-lane body-encode cache (same amortization as the
                // single-lane loop). Each lane owns its own cache.
                let mut body_cache: HashMap<usize, Arc<Vec<u8>>> = HashMap::new();
                for event in lane_rx.iter() {
                    let deltas = topic.evaluate_row_kind_indexed_lane(
                        lane,
                        event.row,
                        event.sequence,
                        event.kind,
                        event.changed_cols.as_deref(),
                    );
                    body_cache.clear();
                    for delta in &deltas {
                        deliver_delta_cached(delta, &registry, &mut body_cache);
                    }
                }
            })?;
        handles.push(handle);
    }

    let dispatch = thread::Builder::new()
        .name(format!("evaluator-dispatch:{}", topic_name))
        .spawn(move || {
            tracing::info!(topic = %topic_name, lanes, "Sharded evaluator started");
            for event in rx.iter() {
                let ev = Arc::new(event);
                for tx in &lane_txs {
                    let _ = tx.send(ev.clone());
                }
            }
            // Source channel closed (topic dropped): release the lane
            // senders so each lane's channel closes and its thread exits.
            drop(lane_txs);
            tracing::info!(topic = %topic_name, "Sharded evaluator dispatcher exiting");
        })?;
    handles.push(dispatch);

    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{new_registry, DeliveryRoute};
    use std::sync::Arc;

    fn make_delta(sub_id: &str, dt: DeltaType) -> Delta {
        Delta {
            subscription_id: sub_id.into(),
            delta_type: dt,
            row: 0,
            sequence: 1,
            row_data: Arc::new(serde_json::Map::new()),
            encoded_body_json: None,
        }
    }

    #[tokio::test]
    async fn dropped_remove_marks_route_degraded() {
        // Capacity-1 channel with no spillover; fill it so the next
        // delivery must hard-drop.
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let route = DeliveryRoute::with_codec(tx.clone(), "/t".into(), Codec::Json);
        // Saturate the queue.
        tx.try_send(crate::session::OutboundFrame::Text("x".into())).unwrap();

        let registry = new_registry();
        registry.insert("s1".into(), route);

        // An Add drop must NOT degrade (self-correcting).
        deliver_delta(&make_delta("s1", DeltaType::Add), &registry);
        assert!(!registry.get("s1").unwrap().is_degraded());

        // A Remove drop is unrecoverable → degraded.
        deliver_delta(&make_delta("s1", DeltaType::Remove), &registry);
        assert!(registry.get("s1").unwrap().is_degraded());
    }

    #[tokio::test]
    async fn dropped_oof_marks_route_degraded() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let route = DeliveryRoute::with_codec(tx.clone(), "/t".into(), Codec::Json);
        tx.try_send(crate::session::OutboundFrame::Text("x".into())).unwrap();
        let registry = new_registry();
        registry.insert("s1".into(), route);

        deliver_delta(&make_delta("s1", DeltaType::Oof), &registry);
        assert!(registry.get("s1").unwrap().is_degraded());
    }

    /// Conflation invariant under sharding: drive a stream of mutations
    /// on a single key through the 4-lane sharded evaluator and assert
    /// the delivered deltas for the subscription arrive in strictly
    /// increasing topic-sequence order. The subscription lives on exactly
    /// one lane, so its conflation state is only ever touched by one
    /// thread — if the dispatcher fanout or per-lane FIFO ordering were
    /// broken, the `seq` field would regress here.
    #[tokio::test]
    async fn sharded_evaluator_preserves_per_sub_ordering() {
        use cq_core::schema::{ColumnType, Schema};
        use cq_core::store::Value;
        use cq_core::topic::{Topic, TopicConfig};
        use std::sync::Arc;

        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity"],
            &[ColumnType::String, ColumnType::Double, ColumnType::Long],
        ));
        let config = TopicConfig {
            name: "/market-data".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        };
        let topic: SharedTopic = Arc::new(Topic::new(config, schema, 1024).with_eval_lanes(4));
        assert_eq!(topic.eval_lanes(), 4);

        // Register the subscription before any live mutations. The
        // initial snapshot is returned to the caller (empty here), not
        // delivered through the registry, so the registry sees only the
        // live deltas the evaluator produces.
        topic
            .subscribe("s1".into(), "SELECT * FROM t")
            .expect("subscribe");

        // Non-conflated JSON route so every delta is delivered as its
        // own frame (no merging) — lets us count and order them.
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(1024);
        let route = DeliveryRoute::with_codec(out_tx, "/market-data".into(), Codec::Json);
        let registry = new_registry();
        registry.insert("s1".into(), route);

        let rx = topic.take_mutation_rx().expect("mutation rx");
        // Detach the evaluator threads: they hold `Arc<Topic>` clones that
        // keep `mutation_tx` alive, so the dispatcher's `rx.iter()` never
        // ends and joining would hang. The test verifies behavior via the
        // delivered frames, not thread teardown.
        let _handles =
            spawn_sharded_evaluator(topic.clone(), rx, registry, 4).expect("spawn evaluators");

        // Publish N mutations to the SAME key with strictly increasing
        // price: first is an Add, the rest Updates. Each allocates the
        // next topic sequence under the write lock, so the channel is
        // already in sequence order.
        const N: u64 = 50;
        for i in 0..N {
            topic.upsert(vec![
                Value::String(Some(compact_str::CompactString::new("AAPL"))),
                Value::Double(100.0 + i as f64),
                Value::Long(10),
            ]);
        }

        // Drain with a timeout rather than joining: the lane threads hold
        // `Arc<Topic>` clones, so `mutation_tx` stays alive and the
        // dispatcher's `rx.iter()` never ends — joining would hang.
        let mut seqs: Vec<u64> = Vec::new();
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(500), out_rx.recv()).await {
                Ok(Some(frame)) => {
                    let text = match frame {
                        crate::session::OutboundFrame::Text(s) => s,
                        _ => panic!("expected JSON text frame"),
                    };
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(v["sid"], "s1");
                    seqs.push(v["seq"].as_u64().expect("seq field"));
                    if seqs.len() as u64 == N {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => break, // timed out waiting for more frames
            }
        }

        assert_eq!(
            seqs.len() as u64,
            N,
            "expected {N} delivered deltas, got {}",
            seqs.len()
        );
        for w in seqs.windows(2) {
            assert!(
                w[0] < w[1],
                "per-subscription sequence regressed: {:?}",
                seqs
            );
        }
    }
}
