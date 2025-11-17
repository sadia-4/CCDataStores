package causalstore.client;

import causalstore.core.CausalMetadata;
import causalstore.core.CausalReadPolicy;
import causalstore.core.VersionVector;
import causalstore.datacenter.DataCenter;
import causalstore.datacenter.ReplicationManager;
import causalstore.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;

public class LinearizableClientSession {
    private static final Logger log = LoggerFactory.getLogger(LinearizableClientSession.class);

    private final String clientId;
    private final DataCenter leader;
    private final ReplicationManager replicationManager;
    private final MetricsCollector metricsCollector;

    public LinearizableClientSession(String clientId,
                                     DataCenter leader,
                                     ReplicationManager replicationManager,
                                     MetricsCollector metricsCollector) {
        this.clientId = clientId;
        this.leader = leader;
        this.replicationManager = replicationManager;
        this.metricsCollector = metricsCollector;
    }

    public void performLinearWrite(String key, String value) {
        log.debug("{} performing linearizable write at {}", clientId, leader.getName());
        CausalMetadata metadata = leader.applyWrite(key, value);
        metricsCollector.recordWrite(leader.getName());
        replicationManager.replicateSync(key, value, leader, metadata);
    }

    public String readLinearizable(String key) {
        long start = System.nanoTime();
        waitUntilAllReplicasCaughtUp();
        String value = leader.read(key);
        metricsCollector.recordRead(CausalReadPolicy.LINEARIZABLE.name(), Duration.ofNanos(System.nanoTime() - start));
        return value;
    }

    private void waitUntilAllReplicasCaughtUp() {
        VersionVector leaderVV = leader.versionVector();
        long startNs = System.nanoTime();
        boolean done;
        do {
            done = true;
            for (DataCenter dc : replicationManager.allDataCenters()) {
                if (!dc.versionVector().dominates(leaderVV)) {
                    done = false;
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    break;
                }
            }
        } while (!done);
        Duration waited = Duration.ofNanos(System.nanoTime() - startNs);
        metricsCollector.recordLinearWait(waited);
    }
}
