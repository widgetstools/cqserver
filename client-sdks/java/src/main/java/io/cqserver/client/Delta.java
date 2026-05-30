package io.cqserver.client;

import java.util.Map;

/** A single subscription update: a snapshot row or a live change. */
public final class Delta {

    /** "sow" (snapshot), "add", "update", "remove", ... */
    public final String deltaType;
    public final String subId;
    public final Map<String, Object> data;
    /** Sequence number, or {@code null} when the server did not supply one. */
    public final Long sequence;

    public Delta(String deltaType, String subId, Map<String, Object> data, Long sequence) {
        this.deltaType = deltaType;
        this.subId = subId;
        this.data = data;
        this.sequence = sequence;
    }

    @Override
    public String toString() {
        return "Delta{type=" + deltaType + ", subId=" + subId
                + ", data=" + data + ", seq=" + sequence + '}';
    }
}
