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

    let route = match registry.get(&delta.subscription_id) {
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

    let frame = if let Some(b) = body {
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
        match route.tx.try_send(frame.clone()) {
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
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Fall through to spillover handling below.
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
    // Either the queue was Full or the route already has spillover
    // backlog. If spillover is wired up, append; otherwise count as a
    // hard drop (legacy behaviour).
    if let Some(sp) = route.spillover.as_ref() {
        match sp.write_frame(&frame) {
            Ok(()) => {
                metrics::counter!("cq_spillover_writes_total").increment(1);
            }
            Err(crate::spillover::SpilloverError::OverCap { .. }) => {
                route.dropped.fetch_add(1, Ordering::Relaxed);
                metrics::counter!(
                    "cq_deltas_dropped_total",
                    "reason" => "spillover_over_cap"
                )
                .increment(1);
            }
            Err(e) => {
                route.dropped.fetch_add(1, Ordering::Relaxed);
                metrics::counter!(
                    "cq_deltas_dropped_total",
                    "reason" => "spillover_io"
                )
                .increment(1);
                tracing::warn!(
                    sub = %delta.subscription_id,
                    error = %e,
                    "Spillover write failed; dropping frame"
                );
            }
        }
    } else {
        route.dropped.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("cq_deltas_dropped_total", "reason" => "queue_full")
            .increment(1);
        tracing::warn!(
            sub = %delta.subscription_id,
            topic = %route.topic,
            "Delta queue full; dropping (consumer can't keep up)"
        );
    }
}

/// Spawn the per-topic evaluator thread. Blocks on the mutation channel;
/// exits when the topic's `Sender` is dropped (i.e., the topic is gone).
pub fn spawn_evaluator(
    topic: SharedTopic,
    rx: Receiver<MutationEvent>,
    registry: SessionRegistry,
) -> JoinHandle<()> {
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
                let deltas =
                    topic.evaluate_row_kind(event.row, event.sequence, event.kind);
                body_cache.clear();
                for delta in &deltas {
                    deliver_delta_cached(delta, &registry, &mut body_cache);
                }
            }
            tracing::info!(topic = %topic_name, "Evaluator thread exiting");
        })
        .expect("evaluator thread spawn")
}
