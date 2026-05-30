package io.cqserver.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Delivers snapshot rows then live deltas via a thread-safe queue. */
public final class Subscription {

    volatile String subId;
    final String cid;
    private final BlockingQueue<Delta> queue = new LinkedBlockingQueue<>();
    /**
     * Highest sequence delivered on this subscription so far. Tracked so a
     * caller can resume from {@code getLastSequence() + 1} (pass it as the
     * {@code bookmark} to a fresh {@code sowAndSubscribe} after a reconnect).
     * Mirrors the TS/Rust SDKs' {@code lastSequence} / {@code last_seq}.
     */
    private volatile long lastSequence = 0;

    Subscription(String subId, String cid) {
        this.subId = subId;
        this.cid = cid;
    }

    public String getSubId() {
        return subId;
    }

    public String getCid() {
        return cid;
    }

    /** Highest sequence number seen on this subscription (0 if none yet). */
    public long getLastSequence() {
        return lastSequence;
    }

    /** Seed the resume point (e.g. from a bookmark on (re)subscribe). */
    void seedLastSequence(long seq) {
        if (seq > lastSequence) {
            lastSequence = seq;
        }
    }

    void push(Delta delta) {
        if (delta.sequence != null && delta.sequence > lastSequence) {
            lastSequence = delta.sequence;
        }
        queue.add(delta);
    }

    /**
     * Block up to {@code timeoutMs} for the next delta. A negative timeout blocks
     * indefinitely. Returns {@code null} on timeout.
     */
    public Delta nextDelta(long timeoutMs) {
        try {
            if (timeoutMs < 0) {
                return queue.take();
            }
            return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
