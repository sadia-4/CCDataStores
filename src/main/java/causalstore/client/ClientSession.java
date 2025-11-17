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
import java.util.Objects;
import java.util.function.Supplier;

public class ClientSession {
    private static final Logger log = LoggerFactory.getLogger(ClientSession.class);
    private static final Duration CAUSAL_POLL = Duration.ofMillis(25);

    private final String clientId;
    private final Supplier<DataCenter> dataCenterSupplier;
    private final ReplicationManager replicationManager;
    private final MetricsCollector metricsCollector;
    private final VersionVector dependencyVector = new VersionVector();

    public ClientSession(String clientId,
                         Supplier<DataCenter> dataCenterSupplier,
                         ReplicationManager replicationManager,
                         MetricsCollector metricsCollector) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.dataCenterSupplier = Objects.requireNonNull(dataCenterSupplier, "dataCenterSupplier");
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.metricsCollector = Objects.requireNonNull(metricsCollector, "metricsCollector");
    }

    public void performWrite(String key, String value) {
        DataCenter dc = dataCenterSupplier.get();
        log.info("{} performing write at {}", clientId, dc.getName());
        CausalMetadata metadata = dc.applyWrite(key, value);
        metricsCollector.recordWrite(dc.getName());
        
        dependencyVector.merge(metadata.versionVector());
        // print dependency vector after merge
        log.info("{} updated dependency vector to {}", clientId, dependencyVector);
        replicationManager.replicate(key, value, dc, metadata);
    }

    public CausalMetadata performWriteWithMetadata(String key, String value) {
        DataCenter dc = dataCenterSupplier.get();
        log.info("{} performing write (with metadata) at {}", clientId, dc.getName());
        CausalMetadata metadata = dc.applyWrite(key, value);
        metricsCollector.recordWrite(dc.getName());

        dependencyVector.merge(metadata.versionVector());
        log.info("{} updated dependency vector to {}", clientId, dependencyVector);
        replicationManager.replicate(key, value, dc, metadata);
        return metadata;
    }

    public String read(String key, CausalReadPolicy policy) {
        DataCenter dc = dataCenterSupplier.get();
        long start = System.nanoTime();
        if (policy == CausalReadPolicy.CAUSAL) {
            waitForDependencies(dc);
        }
        String value = dc.read(key);
        dependencyVector.merge(dc.versionVector());
        Duration latency = Duration.ofNanos(System.nanoTime() - start);
        metricsCollector.recordRead(policy.name(), latency);
        log.info("{} {} read at {}: key={}, value={}", clientId, policy, dc.getName(), key, value);
        return value;
    }

    public void resetDependencies() {
        dependencyVector.clear();
    }

    private void waitForDependencies(DataCenter dc) {
        long startNs = System.nanoTime();
        while (!dc.versionVector().dominates(dependencyVector)) {
            try {
                Thread.sleep(CAUSAL_POLL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Duration waited = Duration.ofNanos(System.nanoTime() - startNs);
        metricsCollector.recordCausalWait(waited);
    }
}
