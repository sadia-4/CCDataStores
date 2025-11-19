package causalstore.client;

import causalstore.core.CausalMetadata;
import causalstore.core.CausalReadPolicy;
import causalstore.core.ReadResult;
import causalstore.core.VersionVector;
import causalstore.datacenter.DataCenter;
import causalstore.datacenter.ReplicationManager;
import causalstore.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
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
    private final Map<String, Long> dependencyVersions = new LinkedHashMap<>();
    private Duration replicationDelayOverride;

    public ClientSession(String clientId,
                         Supplier<DataCenter> dataCenterSupplier,
                         ReplicationManager replicationManager,
                         MetricsCollector metricsCollector) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.dataCenterSupplier = Objects.requireNonNull(dataCenterSupplier, "dataCenterSupplier");
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.metricsCollector = Objects.requireNonNull(metricsCollector, "metricsCollector");
    }

    public CausalMetadata performWrite(String key, String value) {
        DataCenter dc = dataCenterSupplier.get();
        log.info("{} performing write at {}", clientId, dc.getName());
        Map<String, Long> dependencies = dependencyMetadataFor(key);
        Duration delayOverride = consumeReplicationDelayOverride();
        CausalMetadata metadata = dc.applyWrite(key, value, dependencyVector.copy(), dependencies, delayOverride);
        metricsCollector.recordWrite(dc.getName());

        dependencyVector.merge(metadata.versionVector());
        recordSeenKey(key, metadata);
        // print dependency vector after merge
        log.info("{} updated dependency vector to {}", clientId, dependencyVector);
        log.info("{} dependencies for {} -> {}", clientId, key, metadata.dependencies());
        replicationManager.replicate(key, value, dc, metadata);
        return metadata;
    }

  

    public String read(String key, CausalReadPolicy policy) {
        DataCenter dc = dataCenterSupplier.get();
        long start = System.nanoTime();
        if (policy == CausalReadPolicy.CAUSAL) {
            waitForDependencies(dc);
        }
        ReadResult readResult = dc.readWithMetadata(key);
        String value = readResult.value();
        CausalMetadata metadata = readResult.metadata();
        dependencyVector.merge(dc.versionVector());
        recordSeenKey(key, metadata);
        Duration latency = Duration.ofNanos(System.nanoTime() - start);
        metricsCollector.recordRead(policy.name(), latency);
        if (metadata != null) {
            log.info("{} {} read at {}: key={}, value={}, vector={}, globalSeq={}",
                    clientId, policy, dc.getName(), key, value,
                    metadata.versionVector(), metadata.globalSequence());
        } else {
            log.info("{} {} read at {}: key={}, value={}", clientId, policy, dc.getName(), key, value);
        }
        return value;
    }

    public void resetDependencies() {
        dependencyVector.clear();
        dependencyVersions.clear();
    }

    /** Manually register that {@code key} has the given version for future dependencies. */
    public void addDependency(String key, long globalSequence) {
        if (key == null || globalSequence <= 0) return;
        dependencyVersions.put(key, globalSequence);
    }

    /** Declare that the write depends on everything embedded in {@code metadata}. */
    public void addDependency(CausalMetadata metadata) {
        if (metadata == null) return;
        String metaKey = metadata.key();
        if (metaKey != null) {
            dependencyVersions.put(metaKey, metadata.globalSequence());
        }
        metadata.dependencies().forEach(dependencyVersions::put);
    }

    /** Merge dependency metadata manually; useful when you have metadata from another client. */
    public void mergeDependencyMetadata(CausalMetadata metadata) {
        if (metadata == null) return;
        addDependency(metadata);
    }

    public void setNextReplicationDelay(Duration delay) {
        this.replicationDelayOverride = delay;
    }

    private void recordSeenKey(String key, CausalMetadata metadata) {
        if (metadata == null) return;
        String observedKey = metadata.key() != null ? metadata.key() : key;
        if (observedKey != null) {
            dependencyVersions.put(observedKey, metadata.globalSequence());
        }
        metadata.dependencies().forEach(dependencyVersions::put);
    }

    private Map<String, Long> dependencyMetadataFor(String key) {
        if (dependencyVersions.isEmpty()) {
            return Map.of();
        }
        Map<String, Long> dependencies = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : dependencyVersions.entrySet()) {
            if (!entry.getKey().equals(key)) {
                dependencies.put(entry.getKey(), entry.getValue());
            }
        }
        return dependencies;
    }

    private Duration consumeReplicationDelayOverride() {
        Duration override = replicationDelayOverride;
        replicationDelayOverride = null;
        return override;
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
