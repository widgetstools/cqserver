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
    /// Higher values are delivered first. Default 0. Messages of equal
    /// priority preserve FIFO (publish) order.
    priority: i64,
    /// Optional grouping key. All messages sharing a group are pinned
    /// to the same consumer (sticky routing) so their relative order is
    /// preserved at that consumer — the AMPS "message grouping" model.
    group: Option<String>,
}

/// Per-publish delivery options. Defaults give the historical
/// behavior (priority 0, no group) so existing call sites are
/// unaffected.
#[derive(Clone, Copy, Default)]
pub struct PublishOpts<'a> {
    pub priority: i64,
    pub group: Option<&'a str>,
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
    /// Sticky group → consumer routing. A grouped message is delivered
    /// to the consumer its group is pinned to; the pin is established
    /// on first delivery and cleared when that consumer disconnects.
    group_routes: HashMap<String, String>,
}

impl QueueInner {
    /// Insert `msg` into `buffer` keeping it ordered by descending
    /// priority, FIFO within a priority class. The all-equal-priority
    /// case (the overwhelmingly common one) stays O(1): a message whose
    /// priority is `<=` the current tail just appends.
    fn insert_by_priority(&mut self, msg: QueueMessage) {
        let buf = &mut self.buffer;
        match buf.back() {
            Some(tail) if msg.priority <= tail.priority => buf.push_back(msg),
            None => buf.push_back(msg),
            Some(_) => {
                // Some buffered message has lower priority than `msg`.
                // Insert before the first such message.
                let pos = buf
                    .iter()
                    .position(|m| m.priority < msg.priority)
                    .unwrap_or(buf.len());
                buf.insert(pos, msg);
            }
        }
    }

    /// Evict one message to make room under the buffer cap: the oldest
    /// message of the lowest-priority class. The buffer is sorted
    /// descending by priority (FIFO within a class), so the lowest
    /// priority sits at the tail; among that class the oldest is the
    /// first occurrence scanning from the front. For the all-equal
    /// (priority-disabled) case this reduces to dropping the oldest —
    /// the original ring-buffer semantics.
    fn evict_one_for_cap(&mut self) -> bool {
        let buf = &mut self.buffer;
        let Some(lowest) = buf.back().map(|m| m.priority) else {
            return false;
        };
        let pos = buf
            .iter()
            .position(|m| m.priority == lowest)
            .expect("back() exists, so some element has lowest priority");
        buf.remove(pos);
        true
    }
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
    /// Hard cap on buffered (undelivered) messages. When a publish would
    /// push the backlog past this, the *oldest* buffered message is
    /// evicted first (ring-buffer semantics) so memory stays bounded even
    /// when no consumer is connected. Eviction is counted via
    /// `cq_queue_buffer_overflow_drops_total`. Defaults to
    /// [`DEFAULT_MAX_QUEUE_BUFFER`].
    max_buffer: usize,
}

/// Default ceiling on a queue's undelivered backlog. Large enough to
/// ride out a transient consumer outage, small enough that a publisher
/// with no consumer can't exhaust memory. Override per queue via
/// [`Queue::with_max_buffer`].
const DEFAULT_MAX_QUEUE_BUFFER: usize = 1_000_000;

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
                group_routes: HashMap::new(),
            }),
            lease_ms,
            max_delivery_count: 8,
            dlq_name: None,
            max_buffer: DEFAULT_MAX_QUEUE_BUFFER,
        }
    }

    /// Builder: cap the undelivered backlog. Values are floored at 1 so a
    /// queue can always hold at least one message. See `max_buffer`.
    pub fn with_max_buffer(mut self, cap: usize) -> Self {
        self.max_buffer = cap.max(1);
        self
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

    /// Publish `data` with default options (priority 0, no group).
    /// Returns the assigned sequence.
    pub fn publish(
        &self,
        data: serde_json::Value,
        registry: &SessionRegistry,
    ) -> u64 {
        self.publish_with_opts(data, PublishOpts::default(), registry)
    }

    /// Publish `data`, honoring per-message `opts`. Delivers to one
    /// consumer if any are connected (a grouped message sticks to its
    /// group's pinned consumer); otherwise buffers the message in
    /// priority order until a consumer subscribes.
    pub fn publish_with_opts(
        &self,
        data: serde_json::Value,
        opts: PublishOpts<'_>,
        registry: &SessionRegistry,
    ) -> u64 {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let msg = QueueMessage {
            sequence: seq,
            data,
            priority: opts.priority,
            group: opts.group.map(|s| s.to_string()),
        };

        // Opportunistic sweep: cheap to check on every publish if
        // leases are enabled.
        if self.lease_ms.is_some() {
            self.sweep_expired(registry);
        }

        let target = {
            let mut g = self.inner.lock();
            if let Some(sub_id) = self.pick_consumer_for(&mut g, msg.group.as_deref()) {
                Some(sub_id)
            } else {
                // No consumer: buffer it, but bound memory. When at cap,
                // evict the lowest-priority/newest (buffer tail) so a
                // publisher can't OOM the process during a consumer
                // outage without ever starving high-priority work.
                while g.buffer.len() >= self.max_buffer {
                    g.evict_one_for_cap();
                    metrics::counter!(
                        "cq_queue_buffer_overflow_drops_total",
                        "queue" => self.name.clone()
                    )
                    .increment(1);
                }
                g.insert_by_priority(msg.clone());
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

    /// Choose the consumer that should receive a message in `group`.
    /// Ungrouped messages round-robin across consumers. A grouped
    /// message sticks to the consumer its group is pinned to (if still
    /// connected); otherwise it picks the next consumer round-robin and
    /// pins the group there. Returns `None` when no consumer is
    /// connected.
    fn pick_consumer_for(
        &self,
        g: &mut QueueInner,
        group: Option<&str>,
    ) -> Option<String> {
        if g.consumers.is_empty() {
            return None;
        }
        if let Some(group) = group {
            if let Some(pinned) = g.group_routes.get(group) {
                if g.consumers.contains(pinned) {
                    return Some(pinned.clone());
                }
            }
            // Unpinned (or pinned consumer gone): assign round-robin.
            let sub_id = g.consumers.pop_front()?;
            g.consumers.push_back(sub_id.clone());
            g.group_routes.insert(group.to_string(), sub_id.clone());
            return Some(sub_id);
        }
        let sub_id = g.consumers.pop_front()?;
        g.consumers.push_back(sub_id.clone());
        Some(sub_id)
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

    /// Extend the lease on an in-flight delivery by `extra_ms` from
    /// now, giving a slow consumer more time to finish before the
    /// message is redelivered. Returns `true` if a matching live lease
    /// was found and extended. A consumer that knows a task will take a
    /// while sends this instead of letting the lease lapse.
    pub fn extend_lease(&self, delivery_id: u64, extra_ms: u64) -> bool {
        if self.lease_ms.is_none() {
            return false;
        }
        let extended = {
            let mut g = self.inner.lock();
            if let Some(lease) = g.in_flight.get_mut(&delivery_id) {
                lease.expires_at = Instant::now() + Duration::from_millis(extra_ms);
                true
            } else {
                false
            }
        };
        if extended {
            metrics::counter!(
                "cq_queue_lease_extended_total",
                "queue" => self.name.clone()
            )
            .increment(1);
        }
        extended
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
                    self.inner.lock().insert_by_priority(lease.msg);
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
            // Drain in priority order (buffer is already priority-sorted),
            // routing each message through the group-aware selector so
            // grouped messages pin to a single consumer.
            while let Some(msg) = g.buffer.pop_front() {
                let consumer = self
                    .pick_consumer_for(&mut g, msg.group.as_deref())
                    .expect("just pushed a consumer");
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
            // Drop any group pins held by the departing consumer so the
            // next grouped message reassigns to a live consumer.
            g.group_routes.retain(|_, owner| owner != sub_id);
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
    async fn buffer_cap_evicts_oldest() {
        let q = Queue::new("/q").with_max_buffer(3);
        let registry = new_registry();
        // Publish 5 with no consumer; cap is 3 so the 2 oldest evict.
        for i in 0..5 {
            q.publish(serde_json::json!({"i": i}), &registry);
        }
        assert_eq!(*q.stats().get("buffered").unwrap(), 3);

        // The survivors must be the 3 newest (i = 2,3,4), in order.
        let mut rx = add_route(&registry, "sub-1");
        q.add_consumer("sub-1".into(), &registry);
        let mut got = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            let m = parse(frame);
            got.push(m.data.unwrap()["i"].as_i64().unwrap());
        }
        assert_eq!(got, vec![2, 3, 4]);
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
    async fn buffered_messages_drain_in_priority_order() {
        let q = Queue::new("/q");
        let registry = new_registry();
        // Publish out of priority order with no consumer connected.
        for (i, prio) in [(0, 0), (1, 5), (2, 0), (3, 10), (4, 5)] {
            q.publish_with_opts(
                serde_json::json!({ "i": i }),
                PublishOpts { priority: prio, group: None },
                &registry,
            );
        }
        let mut rx = add_route(&registry, "sub-1");
        q.add_consumer("sub-1".into(), &registry);

        let mut got = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            got.push(parse(frame).data.unwrap()["i"].as_i64().unwrap());
        }
        // Highest priority first; FIFO within a priority class:
        // prio10 -> i3; prio5 -> i1 then i4; prio0 -> i0 then i2.
        assert_eq!(got, vec![3, 1, 4, 0, 2]);
    }

    #[tokio::test]
    async fn grouped_messages_stick_to_one_consumer() {
        let q = Queue::new("/q");
        let registry = new_registry();
        let mut rx_a = add_route(&registry, "a");
        let mut rx_b = add_route(&registry, "b");
        q.add_consumer("a".into(), &registry);
        q.add_consumer("b".into(), &registry);

        // Six messages, all group "G" — must all land on one consumer.
        for i in 0..6 {
            q.publish_with_opts(
                serde_json::json!({ "i": i }),
                PublishOpts { priority: 0, group: Some("G") },
                &registry,
            );
        }
        let mut from_a = Vec::new();
        while let Ok(f) = rx_a.try_recv() {
            from_a.push(parse(f).data.unwrap()["i"].as_i64().unwrap());
        }
        let mut from_b = Vec::new();
        while let Ok(f) = rx_b.try_recv() {
            from_b.push(parse(f).data.unwrap()["i"].as_i64().unwrap());
        }
        // One consumer got all 6 (in order), the other got none.
        let (winner, loser) = if from_a.len() == 6 {
            (from_a, from_b)
        } else {
            (from_b, from_a)
        };
        assert_eq!(winner, vec![0, 1, 2, 3, 4, 5]);
        assert!(loser.is_empty());
    }

    #[tokio::test]
    async fn extend_lease_defers_redelivery() {
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

        // Just before expiry, extend the lease by another 500ms.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(q.extend_lease(did, 500), "extend should find live lease");

        // Past the ORIGINAL window — sweep must NOT redeliver.
        tokio::time::sleep(Duration::from_millis(80)).await;
        q.sweep_expired(&registry);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), rx_b.recv())
                .await
                .is_err(),
            "extended lease must not be redelivered yet"
        );

        // A real ack still commits the (extended) lease.
        assert!(q.ack(did));
        assert!(!q.extend_lease(did, 100), "no lease left to extend");
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
