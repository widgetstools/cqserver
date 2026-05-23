//! Message queues — the AMPS "competing consumer" pattern.
//!
//! A `Queue` is a named, in-memory FIFO of `CqMessage` payloads with a
//! round-robin set of consumers. Each published message is delivered to
//! exactly one consumer:
//!
//! - If consumers are ready, the publish hands the message directly to
//!   the next consumer in rotation.
//! - If no consumers are connected, the message is buffered until one
//!   subscribes.
//! - Subscribing drains the buffer round-robin across all current
//!   consumers, so a late joiner can pick up backlogged work.
//!
//! When a `lease_ms` is configured, every delivery carries a
//! `delivery_id` and the queue tracks the in-flight lease in
//! `in_flight`. The consumer commits the lease by sending an `Ack`
//! command with the matching `delivery_id`; if the lease expires
//! before that, the message is redelivered (preferably to a
//! different consumer) with `redelivery_count` incremented.

use crate::session::{encode_frame, SessionRegistry};
use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct QueueMessage {
    sequence: u64,
    data: serde_json::Value,
}

/// One in-flight delivery awaiting `Ack` from the consumer.
#[derive(Clone)]
struct LeaseRecord {
    msg: QueueMessage,
    sub_id: String,
    expires_at: Instant,
    redelivery_count: u32,
}

struct QueueInner {
    buffer: VecDeque<QueueMessage>,
    consumers: VecDeque<String>,
    in_flight: HashMap<u64, LeaseRecord>,
    next_delivery_id: u64,
}

pub struct Queue {
    name: String,
    next_sequence: AtomicU64,
    inner: Mutex<QueueInner>,
    /// Lease window. `None` means leases are disabled (at-most-once
    /// fire-and-forget — the previous behavior). `Some(ms)` enables
    /// at-least-once with redelivery after `ms` of no ack.
    lease_ms: Option<u64>,
    /// Cap on how many times one message may be redelivered before
    /// it's routed to the DLQ (or dropped, if no DLQ is configured).
    /// Default = 8.
    max_delivery_count: u32,
    /// Optional dead-letter queue: when a message exhausts its
    /// `max_delivery_count` it's routed here instead of being
    /// silently dropped. Resolved via the global queue registry by
    /// name, so the DLQ can be any other queue (typically another
    /// `Queue` with no lease — a pure inspection queue).
    dlq_name: Option<String>,
}

impl Queue {
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_lease(name, None)
    }

    /// Construct a queue with optional per-message lease enabled. When
    /// `lease_ms` is `Some`, every delivery is tracked and redelivered
    /// after `lease_ms` of no ack.
    pub fn with_lease(name: impl Into<String>, lease_ms: Option<u64>) -> Self {
        Queue {
            name: name.into(),
            next_sequence: AtomicU64::new(0),
            inner: Mutex::new(QueueInner {
                buffer: VecDeque::new(),
                consumers: VecDeque::new(),
                in_flight: HashMap::new(),
                next_delivery_id: 0,
            }),
            lease_ms,
            max_delivery_count: 8,
            dlq_name: None,
        }
    }

    /// Builder: cap on redelivery attempts. After this many redeliveries
    /// the message is dead-lettered (or dropped if no DLQ is set).
    pub fn with_max_delivery_count(mut self, n: u32) -> Self {
        self.max_delivery_count = n;
        self
    }

    /// Builder: route exhausted messages to a configured DLQ instead
    /// of dropping. The DLQ is looked up by name on the global
    /// `QueueRegistry` at the moment of dead-letter routing, so the
    /// referenced queue can be added before or after this one.
    pub fn with_dlq(mut self, dlq_name: impl Into<String>) -> Self {
        self.dlq_name = Some(dlq_name.into());
        self
    }

    pub fn dlq_name(&self) -> Option<&str> {
        self.dlq_name.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn lease_ms(&self) -> Option<u64> {
        self.lease_ms
    }

    /// Publish `data`. Returns the assigned sequence. Delivers to one
    /// consumer in round-robin order if any are connected; otherwise
    /// buffers the message until a consumer subscribes.
    pub fn publish(
        &self,
        data: serde_json::Value,
        registry: &SessionRegistry,
    ) -> u64 {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let msg = QueueMessage {
            sequence: seq,
            data,
        };

        // Opportunistic sweep: cheap to check on every publish if
        // leases are enabled.
        if self.lease_ms.is_some() {
            self.sweep_expired(registry);
        }

        let target = {
            let mut g = self.inner.lock();
            if let Some(sub_id) = g.consumers.pop_front() {
                g.consumers.push_back(sub_id.clone());
                Some(sub_id)
            } else {
                g.buffer.push_back(msg.clone());
                None
            }
        };

        if let Some(sub_id) = target {
            self.deliver_with_lease(&sub_id, msg, 0, registry);
        }

        metrics::counter!("cq_queue_publish_total", "queue" => self.name.clone())
            .increment(1);
        seq
    }

    /// Deliver `msg` to `sub_id`. If leases are enabled, allocate a
    /// `delivery_id`, install a lease record, and embed the id in the
    /// outbound frame so the consumer's `Ack` can correlate.
    /// `redelivery_count` is 0 on first send, incremented on
    /// subsequent retries.
    fn deliver_with_lease(
        &self,
        sub_id: &str,
        msg: QueueMessage,
        redelivery_count: u32,
        registry: &SessionRegistry,
    ) {
        let did = if let Some(lease_ms) = self.lease_ms {
            let mut g = self.inner.lock();
            g.next_delivery_id += 1;
            let did = g.next_delivery_id;
            g.in_flight.insert(
                did,
                LeaseRecord {
                    msg: msg.clone(),
                    sub_id: sub_id.to_string(),
                    expires_at: Instant::now() + Duration::from_millis(lease_ms),
                    redelivery_count,
                },
            );
            Some(did)
        } else {
            None
        };
        deliver(sub_id, &msg, &self.name, did, registry);
    }

    /// Commit a delivered lease. Returns `true` if a matching lease
    /// was found and removed; `false` otherwise (e.g. duplicate ack
    /// or the lease already expired and got redelivered). Updates the
    /// ack-latency / commit counter.
    pub fn ack(&self, delivery_id: u64) -> bool {
        if self.lease_ms.is_none() {
            return false;
        }
        let removed = {
            let mut g = self.inner.lock();
            g.in_flight.remove(&delivery_id).is_some()
        };
        if removed {
            metrics::counter!(
                "cq_queue_ack_total",
                "queue" => self.name.clone()
            )
            .increment(1);
        }
        removed
    }

    /// Find every in-flight lease whose `expires_at` is in the past
    /// and redeliver its message — preferably to a *different*
    /// consumer than the original (so a stuck or crashed consumer
    /// doesn't trap the message). Returns the number of leases
    /// redelivered, dead-lettered, or dropped.
    pub fn sweep_expired(&self, registry: &SessionRegistry) -> usize {
        self.sweep_expired_with_queues(registry, None)
    }

    /// Variant of `sweep_expired` that has access to the global queue
    /// registry — required for DLQ routing. The basic
    /// `sweep_expired` (used by the in-process unit tests) passes
    /// `None` and routes "max delivery" to a drop+metric path.
    pub fn sweep_expired_with_queues(
        &self,
        registry: &SessionRegistry,
        queues: Option<&QueueRegistry>,
    ) -> usize {
        if self.lease_ms.is_none() {
            return 0;
        }
        let now = Instant::now();
        let expired: Vec<(u64, LeaseRecord)> = {
            let mut g = self.inner.lock();
            let ids: Vec<u64> = g
                .in_flight
                .iter()
                .filter(|(_, lease)| lease.expires_at <= now)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| g.in_flight.remove(&id).map(|r| (id, r)))
                .collect()
        };
        if expired.is_empty() {
            return 0;
        }
        let mut acted = 0;
        for (_did, lease) in expired {
            if lease.redelivery_count + 1 > self.max_delivery_count {
                // Cap exceeded — route to DLQ if configured + resolvable,
                // otherwise drop and bump a counter.
                let dlq = self
                    .dlq_name
                    .as_deref()
                    .and_then(|name| queues.and_then(|qs| qs.get(name).map(|q| q.clone())));
                if let Some(dlq) = dlq {
                    let mut payload = serde_json::Map::new();
                    payload.insert(
                        "original_queue".into(),
                        serde_json::Value::from(self.name.clone()),
                    );
                    payload.insert(
                        "original_sequence".into(),
                        serde_json::Value::from(lease.msg.sequence),
                    );
                    payload.insert(
                        "redelivery_count".into(),
                        serde_json::Value::from(lease.redelivery_count + 1),
                    );
                    payload.insert("payload".into(), lease.msg.data.clone());
                    dlq.publish(serde_json::Value::Object(payload), registry);
                    metrics::counter!(
                        "cq_queue_dlq_routed_total",
                        "queue" => self.name.clone()
                    )
                    .increment(1);
                } else {
                    metrics::counter!(
                        "cq_queue_max_redelivery_dropped_total",
                        "queue" => self.name.clone()
                    )
                    .increment(1);
                }
                acted += 1;
                continue;
            }
            let next_consumer = {
                let mut g = self.inner.lock();
                self.pick_next_consumer_excluding(&mut g, &lease.sub_id)
            };
            match next_consumer {
                Some(sid) => {
                    metrics::counter!(
                        "cq_queue_redelivered_total",
                        "queue" => self.name.clone()
                    )
                    .increment(1);
                    self.deliver_with_lease(
                        &sid,
                        lease.msg,
                        lease.redelivery_count + 1,
                        registry,
                    );
                }
                None => {
                    self.inner.lock().buffer.push_front(lease.msg);
                }
            }
            acted += 1;
        }
        acted
    }

    /// Round-robin step that prefers a consumer other than
    /// `original_sub_id`. Falls back to the original when it's the
    /// only consumer connected.
    fn pick_next_consumer_excluding(
        &self,
        g: &mut QueueInner,
        original_sub_id: &str,
    ) -> Option<String> {
        if g.consumers.is_empty() {
            return None;
        }
        // First pass: rotate looking for someone != original.
        for _ in 0..g.consumers.len() {
            let cand = g.consumers.pop_front()?;
            g.consumers.push_back(cand.clone());
            if cand != original_sub_id {
                return Some(cand);
            }
        }
        // Only the original is connected; return them.
        g.consumers.front().cloned()
    }

    /// Register a new consumer for this queue. Any messages already
    /// buffered are drained immediately, distributed round-robin across
    /// all current consumers (including the newcomer).
    pub fn add_consumer(&self, sub_id: String, registry: &SessionRegistry) {
        let deliveries = {
            let mut g = self.inner.lock();
            g.consumers.push_back(sub_id);
            let mut out = Vec::new();
            while let Some(msg) = g.buffer.pop_front() {
                let consumer = g.consumers.pop_front().expect("just pushed");
                g.consumers.push_back(consumer.clone());
                out.push((consumer, msg));
            }
            out
        };
        for (sid, msg) in deliveries {
            self.deliver_with_lease(&sid, msg, 0, registry);
        }
        metrics::gauge!("cq_queue_consumers", "queue" => self.name.clone())
            .increment(1.0);
    }

    /// Drop a consumer from the round-robin list. In-flight messages
    /// already delivered to that consumer's outbound queue are not
    /// recalled (v2 has no per-message ack/redelivery).
    pub fn remove_consumer(&self, sub_id: &str) {
        let removed = {
            let mut g = self.inner.lock();
            let len = g.consumers.len();
            g.consumers.retain(|s| s != sub_id);
            len != g.consumers.len()
        };
        if removed {
            metrics::gauge!("cq_queue_consumers", "queue" => self.name.clone())
                .decrement(1.0);
        }
    }

    pub fn stats(&self) -> serde_json::Value {
        let g = self.inner.lock();
        serde_json::json!({
            "name": self.name,
            "kind": "queue",
            "buffered": g.buffer.len(),
            "consumers": g.consumers.len(),
            "sequence": self.next_sequence.load(Ordering::Relaxed),
        })
    }
}

pub type SharedQueue = Arc<Queue>;
pub type QueueRegistry = Arc<DashMap<String, SharedQueue>>;

pub fn new_queue_registry() -> QueueRegistry {
    Arc::new(DashMap::new())
}

/// Spawn a background task that periodically sweeps expired leases on
/// `queue`, redelivering, dead-lettering, or dropping as appropriate.
/// Idempotent — safe to call once per queue at startup. The task ticks
/// every `lease_ms / 2` (with a 50 ms floor) so a typical lease
/// expires within ~1.5 ticks. The `queues` registry is needed for
/// DLQ routing; pass `None` if the queue has no DLQ.
pub fn spawn_lease_sweeper(
    queue: SharedQueue,
    registry: SessionRegistry,
    queues: Option<QueueRegistry>,
) {
    let Some(lease_ms) = queue.lease_ms() else {
        return;
    };
    let tick = Duration::from_millis(((lease_ms / 2).max(50)) as u64);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tick).await;
            queue.sweep_expired_with_queues(&registry, queues.as_ref());
        }
    });
}

fn deliver(
    sub_id: &str,
    msg: &QueueMessage,
    queue_name: &str,
    delivery_id: Option<u64>,
    registry: &SessionRegistry,
) {
    let route = match registry.get(sub_id) {
        Some(r) => r,
        None => {
            metrics::counter!(
                "cq_queue_delivery_dropped_total",
                "queue" => queue_name.to_string(),
                "reason" => "consumer_gone",
            )
            .increment(1);
            return;
        }
    };

    let mut cq_msg = CqMessage::new(Command::Publish);
    cq_msg.sub_id = Some(sub_id.to_string());
    cq_msg.topic = Some(queue_name.to_string());
    cq_msg.sequence = Some(msg.sequence);
    cq_msg.data = Some(msg.data.clone());
    cq_msg.delivery_id = delivery_id;
    let frame = match encode_frame(route.codec, &cq_msg) {
        Some(f) => f,
        None => return,
    };
    match route.tx.try_send(frame) {
        Ok(()) => {
            metrics::counter!("cq_queue_delivered_total", "queue" => queue_name.to_string())
                .increment(1);
        }
        Err(_) => {
            route.dropped.fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "cq_queue_delivery_dropped_total",
                "queue" => queue_name.to_string(),
                "reason" => "outbound_full",
            )
            .increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{new_registry, DeliveryRoute, OutboundFrame};
    use cq_protocol::serialization::Codec;
    use tokio::sync::mpsc;

    fn add_route(
        registry: &SessionRegistry,
        sub_id: &str,
    ) -> mpsc::Receiver<OutboundFrame> {
        let (tx, rx) = mpsc::channel::<OutboundFrame>(64);
        registry.insert(
            sub_id.into(),
            DeliveryRoute::with_codec(tx, "/q".into(), Codec::Json),
        );
        rx
    }

    fn parse(frame: OutboundFrame) -> CqMessage {
        serde_json::from_slice(frame.as_bytes()).unwrap()
    }

    #[tokio::test]
    async fn publish_with_no_consumers_buffers() {
        let q = Queue::new("/q");
        let registry = new_registry();
        let seq = q.publish(serde_json::json!({"x":1}), &registry);
        assert_eq!(seq, 1);
        // Nothing delivered yet — no consumers.
        let stats = q.stats();
        assert_eq!(stats.get("buffered").unwrap(), 1);
    }

    #[tokio::test]
    async fn late_consumer_drains_buffer() {
        let q = Queue::new("/q");
        let registry = new_registry();
        for i in 0..3 {
            q.publish(serde_json::json!({"i": i}), &registry);
        }
        let mut rx = add_route(&registry, "sub-1");
        q.add_consumer("sub-1".into(), &registry);

        let mut seqs = Vec::new();
        while let Some(frame) =
            tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
                .await
                .unwrap_or(None)
        {
            let m = parse(frame);
            seqs.push(m.sequence.unwrap());
        }
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn round_robin_across_two_consumers() {
        let q = Queue::new("/q");
        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        let mut rx_b = add_route(&registry, "b");
        q.add_consumer("a".into(), &registry);
        q.add_consumer("b".into(), &registry);

        for i in 0..6 {
            q.publish(serde_json::json!({"i": i}), &registry);
        }

        let mut from_a = Vec::new();
        let mut from_b = Vec::new();
        for _ in 0..3 {
            let fa = rx_a.recv().await.unwrap();
            from_a.push(parse(fa).sequence.unwrap());
            let fb = rx_b.recv().await.unwrap();
            from_b.push(parse(fb).sequence.unwrap());
        }
        // Each consumer should have received exactly 3 messages.
        assert_eq!(from_a.len(), 3);
        assert_eq!(from_b.len(), 3);
        // The full set of sequences is 1..=6, partitioned across the two.
        let mut all: Vec<u64> = from_a.iter().chain(from_b.iter()).copied().collect();
        all.sort();
        assert_eq!(all, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn lease_redelivers_to_other_consumer_after_expiry() {
        // 100ms lease, two consumers. Publish once, consumer A
        // receives and never acks; after the lease window, B
        // receives the same payload via redelivery.
        let q = Arc::new(Queue::with_lease("/q", Some(100)));
        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        let mut rx_b = add_route(&registry, "b");
        q.add_consumer("a".into(), &registry);
        q.add_consumer("b".into(), &registry);

        let seq = q.publish(serde_json::json!({ "x": 1 }), &registry);
        assert_eq!(seq, 1);

        // A receives first, with a delivery_id stamped on the frame.
        let frame_a = tokio::time::timeout(Duration::from_millis(100), rx_a.recv())
            .await
            .unwrap()
            .expect("a got nothing");
        let msg_a = parse(frame_a);
        let did_a = msg_a.delivery_id.expect("missing delivery_id");
        assert!(did_a > 0);
        // B has nothing yet.
        assert!(tokio::time::timeout(Duration::from_millis(50), rx_b.recv())
            .await
            .is_err());

        // Let the lease expire + sweep.
        tokio::time::sleep(Duration::from_millis(150)).await;
        q.sweep_expired(&registry);

        // B should now receive the redelivered message.
        let frame_b = tokio::time::timeout(Duration::from_millis(200), rx_b.recv())
            .await
            .unwrap()
            .expect("b got nothing on redelivery");
        let msg_b = parse(frame_b);
        // Same payload + sequence; different delivery_id.
        assert_eq!(msg_b.sequence, msg_a.sequence);
        let did_b = msg_b.delivery_id.expect("missing delivery_id");
        assert_ne!(did_a, did_b);
    }

    #[tokio::test]
    async fn lease_ack_prevents_redelivery() {
        let q = Arc::new(Queue::with_lease("/q", Some(100)));
        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        let mut rx_b = add_route(&registry, "b");
        q.add_consumer("a".into(), &registry);
        q.add_consumer("b".into(), &registry);

        q.publish(serde_json::json!({ "x": 1 }), &registry);

        let frame_a = tokio::time::timeout(Duration::from_millis(100), rx_a.recv())
            .await
            .unwrap()
            .expect("a got nothing");
        let did = parse(frame_a).delivery_id.expect("missing did");
        // Ack before lease expires.
        assert!(q.ack(did), "first ack should remove the lease");
        // Duplicate ack returns false.
        assert!(!q.ack(did));

        tokio::time::sleep(Duration::from_millis(150)).await;
        q.sweep_expired(&registry);
        // B should NOT have received anything — the lease was acked.
        assert!(tokio::time::timeout(Duration::from_millis(100), rx_b.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn dlq_receives_messages_after_max_redelivery() {
        // max_delivery=1, lease=50ms, no consumers ever ack. After
        // one redelivery attempt the cap is exceeded → DLQ.
        let queues = new_queue_registry();
        let dlq = Arc::new(Queue::new("/dlq"));
        queues.insert("/dlq".into(), dlq.clone());
        let main_q = Arc::new(
            Queue::with_lease("/work", Some(50))
                .with_max_delivery_count(1)
                .with_dlq("/dlq"),
        );
        queues.insert("/work".into(), main_q.clone());

        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        main_q.add_consumer("a".into(), &registry);

        main_q.publish(serde_json::json!({"task": "stuck"}), &registry);
        // A receives it but never acks.
        let _f = tokio::time::timeout(Duration::from_millis(100), rx_a.recv())
            .await
            .unwrap()
            .expect("a got nothing");

        // First sweep redelivers (count → 1), still under cap.
        tokio::time::sleep(Duration::from_millis(60)).await;
        main_q.sweep_expired_with_queues(&registry, Some(&queues));
        // A receives the redelivery (only consumer).
        let _f = tokio::time::timeout(Duration::from_millis(100), rx_a.recv())
            .await
            .unwrap()
            .expect("a got nothing on redelivery");

        // Second sweep — count would become 2, exceeds cap → DLQ.
        tokio::time::sleep(Duration::from_millis(60)).await;
        let acted = main_q.sweep_expired_with_queues(&registry, Some(&queues));
        assert_eq!(acted, 1, "expected one expired lease this sweep");
        // DLQ should now hold the dead-lettered message in its buffer.
        let dlq_stats = dlq.stats();
        let buffered = dlq_stats.get("buffered").and_then(|v| v.as_u64()).unwrap_or(0);
        assert_eq!(buffered, 1, "DLQ should hold 1 dead-lettered message");
    }

    #[tokio::test]
    async fn removing_consumer_stops_delivery_to_them() {
        let q = Queue::new("/q");
        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        let mut rx_b = add_route(&registry, "b");
        q.add_consumer("a".into(), &registry);
        q.add_consumer("b".into(), &registry);

        q.remove_consumer("a");
        for _ in 0..4 {
            q.publish(serde_json::json!({"k":"v"}), &registry);
        }

        // a gets nothing.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx_a.recv())
                .await
                .is_err()
        );
        // b gets all four.
        let mut got = 0;
        while let Some(_f) =
            tokio::time::timeout(std::time::Duration::from_millis(100), rx_b.recv())
                .await
                .unwrap_or(None)
        {
            got += 1;
            if got >= 4 {
                break;
            }
        }
        assert_eq!(got, 4);
    }
}
