import io.cqserver.client.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

/**
 * CQServer Java SDK quickstart.
 *
 * <p>Run a server first, e.g.:
 * <pre>target/release/cqserver --config clients/sdk-smoke.toml</pre>
 *
 * <p>Then compile the SDK and run this single file in source mode:
 * <pre>
 * javac -d build $(find client-sdks/java/src -name '*.java')
 * java -cp build client-sdks/java/examples/Quickstart.java [host] [port]
 * </pre>
 */
public class Quickstart {

    static final String TOPIC = "/sdk-test";

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9099;

        try (CqClient c = CqClient.connect(host, port)) {
            int version = c.logon(); // anonymous handshake
            System.out.println("connected, negotiated protocol version " + version);

            Map<String, Object> aaa = new LinkedHashMap<>();
            aaa.put("k", "AAA");
            aaa.put("v", 1);
            long seq = c.publish(TOPIC, aaa);
            System.out.println("published AAA -> seq " + seq);

            // Open a live subscription (snapshot + deltas) before the next write.
            Subscription sub = c.sowAndSubscribe(TOPIC);

            // Drain the snapshot (one row so far).
            int snap = 0;
            while (true) {
                Delta d = sub.nextDelta(500);
                if (d == null) {
                    break;
                }
                if ("sow".equals(d.deltaType)) {
                    snap++;
                    System.out.println("  snapshot row: " + d.data);
                } else {
                    break;
                }
            }
            System.out.println("snapshot delivered " + snap + " row(s)");

            // A live publish should arrive on the subscription as an add/update.
            Map<String, Object> bbb = new LinkedHashMap<>();
            bbb.put("k", "BBB");
            bbb.put("v", 2);
            c.publish(TOPIC, bbb);
            Delta d = sub.nextDelta(2000);
            System.out.println("live delta: type=" + (d != null ? d.deltaType : null)
                    + " data=" + (d != null ? d.data : null));

            // One-shot SOW shows both rows.
            List<Map<String, Object>> rows = c.sow(TOPIC);
            List<String> keys = new ArrayList<>();
            for (Map<String, Object> r : rows) {
                Object k = r.get("k");
                keys.add(k != null ? k.toString() : null);
            }
            Collections.sort(keys);
            System.out.println("sow returned " + rows.size() + " rows: " + keys);

            c.unsubscribe(sub);
        }
    }
}
