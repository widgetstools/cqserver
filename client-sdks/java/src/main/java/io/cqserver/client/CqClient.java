package io.cqserver.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CQServer client over TCP (length-prefixed JSON frames).
 *
 * <p>Faithful port of the proven Python SDK. A single background reader thread
 * processes frames in arrival order, waking RPC waiters and routing snapshot /
 * live deltas to subscriptions.
 */
public final class CqClient implements AutoCloseable {

    /** Top bit of the u32 length prefix flags a zstd-compressed body. */
    private static final long COMPRESSED_FLAG = 0x80000000L;
    private static final int MAX_FRAME = 16 * 1024 * 1024;

    /** Wire protocol versions this SDK understands. */
    private static final List<Object> SUPPORTED_VERSIONS = List.of(1L);

    /**
     * Connection options. Mirrors the TS SDK's {@code ClientOptions} and the
     * Rust SDK's {@code ClientConfig}. All fields have sensible defaults; use
     * the fluent setters to override.
     */
    public static final class Options {
        int connectTimeoutMs = 5000;
        /**
         * If &gt; 0, the client sends a {@code heartbeat} frame this often to
         * keep the server-side idle timer alive. cqserver disconnects peers
         * that stay silent past its idle timeout (~65s default), so
         * subscriber-only clients must heartbeat or they get kicked roughly
         * every minute. {@code 0} disables. Default 25_000 (25s).
         */
        long heartbeatIntervalMs = 25_000;

        public Options connectTimeoutMs(int ms) {
            this.connectTimeoutMs = ms;
            return this;
        }

        public Options heartbeatIntervalMs(long ms) {
            this.heartbeatIntervalMs = ms;
            return this;
        }
    }

    private final Socket sock;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Object sendLock = new Object();
    private final AtomicLong cidCounter = new AtomicLong(0);
    private volatile int protocolVersion = 1;
    /** The URL this client actually connected to (set by {@code connectUrl} / {@code connectAny}). */
    private volatile String activeUrl;
    /** Optional app callback fired once when the reader loop exits (disconnect). */
    private volatile Runnable onCloseCallback;
    private final Thread heartbeatThread;

    // cid -> Pending (RPC waiters: publish, logon).
    private final Map<String, Pending> pending = new ConcurrentHashMap<>();
    // cid -> Subscription, before the server assigns a sid (promoted on ack).
    private final Map<String, Subscription> pendingSubs = new ConcurrentHashMap<>();
    // sid -> Subscription (live routing).
    private final Map<String, Subscription> subs = new ConcurrentHashMap<>();
    // cid -> snapshot row buffer (one-shot sow).
    private final Map<String, List<Map<String, Object>>> snapshotBuffers = new ConcurrentHashMap<>();
    // cid/sid -> snapshot completion (carries an optional error string).
    private final Map<String, Pending> snapshotDone = new ConcurrentHashMap<>();

    private volatile boolean closed = false;
    private final Thread reader;

    private CqClient(Socket sock, Options opts) throws IOException {
        this.sock = sock;
        this.in = new DataInputStream(new java.io.BufferedInputStream(sock.getInputStream()));
        this.out = new DataOutputStream(new java.io.BufferedOutputStream(sock.getOutputStream()));
        this.reader = new Thread(this::readLoop, "cqclient-reader");
        this.reader.setDaemon(true);
        this.reader.start();
        if (opts.heartbeatIntervalMs > 0) {
            final long interval = opts.heartbeatIntervalMs;
            this.heartbeatThread = new Thread(() -> heartbeatLoop(interval), "cqclient-heartbeat");
            this.heartbeatThread.setDaemon(true);
            this.heartbeatThread.start();
        } else {
            this.heartbeatThread = null;
        }
    }

    // ---- connection -------------------------------------------------

    public static CqClient connect(String host, int port) throws IOException {
        return connect(host, port, new Options());
    }

    public static CqClient connect(String host, int port, int timeoutMs) throws IOException {
        return connect(host, port, new Options().connectTimeoutMs(timeoutMs));
    }

    public static CqClient connect(String host, int port, Options opts) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), opts.connectTimeoutMs);
        s.setSoTimeout(0);
        s.setTcpNoDelay(true);
        return new CqClient(s, opts);
    }

    /** Accepts {@code tcp://host:port}. */
    public static CqClient connectUrl(String url) throws IOException {
        return connectUrl(url, new Options());
    }

    public static CqClient connectUrl(String url, int timeoutMs) throws IOException {
        return connectUrl(url, new Options().connectTimeoutMs(timeoutMs));
    }

    public static CqClient connectUrl(String url, Options opts) throws IOException {
        if (!url.startsWith("tcp://")) {
            throw new CqException("unsupported url (tcp:// only): " + url);
        }
        String hostport = url.substring("tcp://".length());
        int idx = hostport.lastIndexOf(':');
        if (idx < 0) {
            throw new CqException("missing port in url: " + url);
        }
        String host = hostport.substring(0, idx);
        int port = Integer.parseInt(hostport.substring(idx + 1));
        CqClient c = connect(host, port, opts);
        c.activeUrl = url;
        return c;
    }

    /**
     * HA initial-connect failover: try each {@code tcp://host:port} URL in a
     * randomised order and return the first that connects. Randomisation
     * matters when many clients start at once — without it every client would
     * hammer the first URL. Mirrors the TS/Rust SDKs' {@code connectAny} /
     * {@code connect_any}.
     *
     * <p>This is <em>initial-connect</em> failover only; live reconnect after a
     * mid-stream disconnect is the caller's concern (use {@link #onClose} plus
     * {@link Subscription#getLastSequence()} to resume). Throws the last error
     * if every URL fails.
     */
    public static CqClient connectAny(List<String> urls) throws IOException {
        return connectAny(urls, new Options());
    }

    public static CqClient connectAny(List<String> urls, Options opts) throws IOException {
        if (urls == null || urls.isEmpty()) {
            throw new CqException("connectAny: empty url list");
        }
        int[] order = shuffledIndices(urls.size());
        IOException lastErr = null;
        for (int i : order) {
            try {
                return connectUrl(urls.get(i), opts);
            } catch (IOException | CqException e) {
                lastErr = (e instanceof IOException) ? (IOException) e : new IOException(e);
            }
        }
        if (lastErr != null) {
            throw lastErr;
        }
        throw new CqException("connectAny: all URLs failed");
    }

    /** Fisher-Yates permutation of {@code [0, n)}. Matches the TS/Rust SDKs. */
    private static int[] shuffledIndices(int n) {
        int[] idx = new int[n];
        for (int i = 0; i < n; i++) {
            idx[i] = i;
        }
        for (int i = n - 1; i > 0; i--) {
            int j = ThreadLocalRandom.current().nextInt(i + 1);
            int tmp = idx[i];
            idx[i] = idx[j];
            idx[j] = tmp;
        }
        return idx;
    }

    /** The URL this client actually connected to, or {@code null} if connected by host/port. */
    public String getActiveUrl() {
        return activeUrl;
    }

    /**
     * Register a callback fired exactly once when the background reader loop
     * exits (peer closed, socket error, or local {@link #close()}). Use it to
     * drive an application-level reconnect; pair it with
     * {@link Subscription#getLastSequence()} to resume each subscription.
     */
    public void onClose(Runnable cb) {
        this.onCloseCallback = cb;
    }

    @Override
    public void close() {
        closed = true;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        try {
            sock.shutdownInput();
        } catch (IOException ignored) {
            // already closed
        }
        try {
            sock.shutdownOutput();
        } catch (IOException ignored) {
            // already closed
        }
        try {
            sock.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    // ---- framing ----------------------------------------------------

    private void send(Map<String, Object> msg) {
        byte[] body = Json.write(msg).getBytes(StandardCharsets.UTF_8);
        if (body.length > MAX_FRAME) {
            throw new CqException("frame too large: " + body.length + " bytes");
        }
        synchronized (sendLock) {
            try {
                out.writeInt(body.length);
                out.write(body);
                out.flush();
            } catch (IOException e) {
                throw new CqException("send failed", e);
            }
        }
    }

    private void readLoop() {
        try {
            while (true) {
                int raw = in.readInt();
                long lenField = raw & 0xFFFFFFFFL;
                if ((lenField & COMPRESSED_FLAG) != 0) {
                    throw new CqException("received compressed frame but no codec negotiated");
                }
                int length = (int) (lenField & ~COMPRESSED_FLAG);
                if (length > MAX_FRAME) {
                    throw new CqException("frame too large: " + length);
                }
                byte[] payload = new byte[length];
                in.readFully(payload);
                Map<String, Object> msg = Json.parseObject(new String(payload, StandardCharsets.UTF_8));
                dispatch(msg);
            }
        } catch (EOFException e) {
            // peer closed cleanly
        } catch (IOException | CqException e) {
            // socket error / protocol violation — fall through to wake waiters
        } finally {
            failAllWaiters();
            Runnable cb = onCloseCallback;
            if (cb != null) {
                try {
                    cb.run();
                } catch (RuntimeException ignored) {
                    // app callback must not break teardown
                }
            }
        }
    }

    private void failAllWaiters() {
        for (Pending p : pending.values()) {
            p.complete(null);
        }
        for (Pending p : snapshotDone.values()) {
            p.error = "connection closed";
            p.completeError();
        }
    }

    /** Background keepalive: fire-and-forget heartbeats until the socket closes. */
    private void heartbeatLoop(long intervalMs) {
        while (!closed) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (closed) {
                return;
            }
            try {
                Map<String, Object> hb = new LinkedHashMap<>();
                hb.put("c", "heartbeat");
                send(hb);
            } catch (CqException e) {
                // socket gone — the reader loop will surface the disconnect
                return;
            }
        }
    }

    /** Send a single heartbeat frame (fire-and-forget keepalive). */
    public void heartbeat() {
        Map<String, Object> hb = new LinkedHashMap<>();
        hb.put("c", "heartbeat");
        send(hb);
    }

    // ---- dispatch ---------------------------------------------------

    private void dispatch(Map<String, Object> msg) {
        Object cmd = msg.get("c");
        if (!(cmd instanceof String)) {
            return;
        }
        switch ((String) cmd) {
            case "ack":
                onAck(msg);
                break;
            case "sow":
                onSowRow(msg);
                break;
            case "sow_batch":
                onSowBatch(msg);
                break;
            case "group_end":
                onGroupEnd(msg);
                break;
            case "publish":
                onDelta(msg);
                break;
            default:
                // group_begin / heartbeat / schema_change: nothing to do here.
                break;
        }
    }

    private void onAck(Map<String, Object> msg) {
        String cid = asString(msg.get("cid"));
        String sid = asString(msg.get("sid"));
        // Promote a pre-registered subscription into the live routing map FIRST,
        // so same-batch snapshot rows route correctly.
        if (cid != null && sid != null) {
            Subscription sub = pendingSubs.remove(cid);
            if (sub != null) {
                sub.subId = sid;
                subs.put(sid, sub);
            }
        }
        if (cid != null) {
            // Error ack for an in-flight SOW: no group_end will arrive.
            if ("error".equals(asString(msg.get("s")))) {
                Pending p = snapshotDone.remove(cid);
                if (p != null) {
                    String reason = asString(msg.get("r"));
                    p.error = reason != null ? reason : "server error";
                    p.completeError();
                }
            }
            Pending p = pending.remove(cid);
            if (p != null) {
                p.complete(msg);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void onSowRow(Map<String, Object> msg) {
        String sid = asString(msg.get("sid"));
        Object data = msg.get("d");
        if (sid == null || !(data instanceof Map)) {
            return;
        }
        Map<String, Object> row = (Map<String, Object>) data;
        List<Map<String, Object>> buf = snapshotBuffers.get(sid);
        Subscription sub = subs.get(sid);
        if (buf != null) {
            buf.add(row);
        }
        if (sub != null) {
            sub.push(new Delta("sow", sid, row, asLong(msg.get("seq"))));
        }
    }

    @SuppressWarnings("unchecked")
    private void onSowBatch(Map<String, Object> msg) {
        String sid = asString(msg.get("sid"));
        Object rows = msg.get("d");
        if (sid == null || !(rows instanceof List)) {
            return;
        }
        List<Map<String, Object>> buf = snapshotBuffers.get(sid);
        Subscription sub = subs.get(sid);
        for (Object rowObj : (List<Object>) rows) {
            if (!(rowObj instanceof Map)) {
                continue;
            }
            Map<String, Object> row = (Map<String, Object>) rowObj;
            if (buf != null) {
                buf.add(row);
            }
            if (sub != null) {
                sub.push(new Delta("sow", sid, row, null));
            }
        }
    }

    private void onGroupEnd(Map<String, Object> msg) {
        String sid = asString(msg.get("sid"));
        if (sid == null) {
            return;
        }
        Pending p = snapshotDone.remove(sid);
        if (p != null) {
            p.error = null; // success
            p.completeError();
        }
    }

    @SuppressWarnings("unchecked")
    private void onDelta(Map<String, Object> msg) {
        String sid = asString(msg.get("sid"));
        if (sid == null) {
            return;
        }
        Subscription sub = subs.get(sid);
        if (sub == null) {
            return;
        }
        Object data = msg.get("d");
        Map<String, Object> rowData =
                (data instanceof Map) ? (Map<String, Object>) data : new LinkedHashMap<>();
        String dt = asString(msg.get("dt"));
        sub.push(new Delta(dt != null ? dt : "update", sid, rowData, asLong(msg.get("seq"))));
    }

    // ---- helpers ----------------------------------------------------

    private String nextCid() {
        return Long.toString(cidCounter.incrementAndGet());
    }

    private Map<String, Object> rpc(Map<String, Object> msg, long timeoutMs) {
        String cid = asString(msg.get("cid"));
        if (cid == null) {
            cid = nextCid();
            msg.put("cid", cid);
        }
        Pending p = new Pending();
        pending.put(cid, p);
        send(msg);
        Map<String, Object> resp;
        if (!p.await(timeoutMs)) {
            pending.remove(cid);
            throw new CqException("timed out waiting for ack");
        }
        resp = p.response;
        if (resp == null) {
            throw new CqException("connection closed");
        }
        if ("error".equals(asString(resp.get("s")))) {
            String reason = asString(resp.get("r"));
            throw new CqException(reason != null ? reason : "server error");
        }
        return resp;
    }

    // ---- API --------------------------------------------------------

    public int getProtocolVersion() {
        return protocolVersion;
    }

    /** Anonymous handshake (no credentials). Returns the negotiated version. */
    public int logon() {
        return logon(null, null, null, null);
    }

    /**
     * Authenticate and negotiate the protocol version. Pass {@code null} for any
     * argument that does not apply. Returns the negotiated protocol version.
     */
    public int logon(String user, String password, String token, String clientName) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "logon");
        msg.put("pv", SUPPORTED_VERSIONS);
        Map<String, Object> creds = new LinkedHashMap<>();
        if (token != null) {
            creds.put("token", token);
        } else if (user != null) {
            creds.put("user", user);
            creds.put("password", password != null ? password : "");
        }
        if (!creds.isEmpty()) {
            msg.put("d", creds);
        }
        if (clientName != null) {
            msg.put("client", clientName);
        }
        Map<String, Object> resp = rpc(msg, 5000);
        Object pv = resp.get("pv");
        if (pv instanceof List && !((List<?>) pv).isEmpty()) {
            Long v = asLong(((List<?>) pv).get(0));
            if (v != null) {
                protocolVersion = v.intValue();
            }
        }
        return protocolVersion;
    }

    /** Publish/upsert a record. Returns the assigned sequence. */
    public long publish(String topic, Map<String, Object> data) {
        return publish(topic, data, 5000);
    }

    public long publish(String topic, Map<String, Object> data, long timeoutMs) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "publish");
        msg.put("t", topic);
        msg.put("d", data);
        Map<String, Object> resp = rpc(msg, timeoutMs);
        Long seq = asLong(resp.get("seq"));
        return seq != null ? seq : 0L;
    }

    /** Sparse update: merge {@code data} (key + changed fields) into the row. */
    public long deltaPublish(String topic, Map<String, Object> data) {
        return deltaPublish(topic, data, 5000);
    }

    public long deltaPublish(String topic, Map<String, Object> data, long timeoutMs) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "delta_publish");
        msg.put("t", topic);
        msg.put("d", data);
        Map<String, Object> resp = rpc(msg, timeoutMs);
        Long seq = asLong(resp.get("seq"));
        return seq != null ? seq : 0L;
    }

    /** One-shot SOW query. Returns the snapshot rows. */
    public List<Map<String, Object>> sow(String topic) {
        return sow(topic, null, 10000);
    }

    public List<Map<String, Object>> sow(String topic, String filter) {
        return sow(topic, filter, 10000);
    }

    public List<Map<String, Object>> sow(String topic, String filter, long timeoutMs) {
        String cid = nextCid();
        Pending done = new Pending();
        snapshotBuffers.put(cid, new ArrayList<>());
        snapshotDone.put(cid, done);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "sow");
        msg.put("cid", cid);
        msg.put("t", topic);
        if (filter != null) {
            msg.put("f", filter);
        }
        send(msg);
        if (!done.await(timeoutMs)) {
            snapshotBuffers.remove(cid);
            snapshotDone.remove(cid);
            throw new CqException("timed out waiting for SOW group_end");
        }
        List<Map<String, Object>> rows = snapshotBuffers.remove(cid);
        if (rows == null) {
            rows = new ArrayList<>();
        }
        if (done.error != null) {
            throw new CqException(done.error);
        }
        return rows;
    }

    /** Run an arbitrary SOW SQL query (GROUP BY / aggregates). */
    public List<Map<String, Object>> sowSql(String topic, String sql) {
        return sowSql(topic, sql, 10000);
    }

    public List<Map<String, Object>> sowSql(String topic, String sql, long timeoutMs) {
        String cid = nextCid();
        Pending done = new Pending();
        snapshotBuffers.put(cid, new ArrayList<>());
        snapshotDone.put(cid, done);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "sow");
        msg.put("cid", cid);
        msg.put("t", topic);
        msg.put("sql", sql);
        send(msg);
        if (!done.await(timeoutMs)) {
            snapshotBuffers.remove(cid);
            snapshotDone.remove(cid);
            throw new CqException("timed out waiting for SOW group_end");
        }
        List<Map<String, Object>> rows = snapshotBuffers.remove(cid);
        if (rows == null) {
            rows = new ArrayList<>();
        }
        if (done.error != null) {
            throw new CqException(done.error);
        }
        return rows;
    }

    /**
     * Snapshot + continuous deltas. Snapshot rows arrive as deltas with
     * deltaType == "sow"; live changes as "add"/"update"/"remove".
     */
    public Subscription sowAndSubscribe(String topic) {
        return sowAndSubscribe(topic, null, null, null);
    }

    public Subscription sowAndSubscribe(String topic, String filter, Long bookmark) {
        return sowAndSubscribe(topic, filter, bookmark, null);
    }

    /**
     * Snapshot + continuous deltas with an optional server-side conflation
     * interval. When {@code conflationMs} is non-null the server coalesces
     * per-row updates (latest-value) and flushes every {@code conflationMs},
     * clamped to the server's ceiling. {@code 0} opts out even if the topic
     * configures a baseline; {@code null} uses the topic default.
     */
    public Subscription sowAndSubscribe(String topic, String filter, Long bookmark, Long conflationMs) {
        String cid = nextCid();
        Subscription sub = new Subscription("", cid);
        if (bookmark != null) {
            sub.seedLastSequence(bookmark);
        }
        pendingSubs.put(cid, sub);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "sow_and_subscribe");
        msg.put("cid", cid);
        msg.put("t", topic);
        if (filter != null) {
            msg.put("f", filter);
        }
        if (bookmark != null) {
            msg.put("bm", bookmark);
        }
        if (conflationMs != null) {
            msg.put("o", "conflation=" + conflationMs);
        }
        send(msg);
        return sub;
    }

    /** Live deltas only (no initial snapshot). */
    public Subscription subscribe(String topic) {
        return subscribe(topic, null, null);
    }

    public Subscription subscribe(String topic, String filter) {
        return subscribe(topic, filter, null);
    }

    /** Live deltas only, with an optional server-side conflation interval (ms). */
    public Subscription subscribe(String topic, String filter, Long conflationMs) {
        String cid = nextCid();
        Subscription sub = new Subscription("", cid);
        pendingSubs.put(cid, sub);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("c", "subscribe");
        msg.put("cid", cid);
        msg.put("t", topic);
        if (filter != null) {
            msg.put("f", filter);
        }
        if (conflationMs != null) {
            msg.put("o", "conflation=" + conflationMs);
        }
        send(msg);
        return sub;
    }

    public void unsubscribe(Subscription sub) {
        String sid = sub.subId;
        if (sid != null && !sid.isEmpty()) {
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("c", "unsubscribe");
            msg.put("sid", sid);
            send(msg);
            subs.remove(sid);
        }
    }

    // ---- coercion helpers -------------------------------------------

    private static String asString(Object o) {
        return (o instanceof String) ? (String) o : null;
    }

    private static Long asLong(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        return null;
    }

    // ---- RPC waiter -------------------------------------------------

    /** A one-shot completion gate carrying either an ack response or an error string. */
    private static final class Pending {
        private final CountDownLatch latch = new CountDownLatch(1);
        volatile Map<String, Object> response;
        volatile String error;

        boolean await(long timeoutMs) {
            try {
                if (timeoutMs < 0) {
                    latch.await();
                    return true;
                }
                return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        void complete(Map<String, Object> resp) {
            this.response = resp;
            latch.countDown();
        }

        void completeError() {
            latch.countDown();
        }
    }
}
