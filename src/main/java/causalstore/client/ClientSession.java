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
    private final Map<String, VersionVector> dependencyVersions = new LinkedHashMap<>();
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
        Map<String, VersionVector> dependencies = dependencyMetadataFor(key);
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

    public CausalMetadata performWriteWithMetadata(String key, String value) {
        DataCenter dc = dataCenterSupplier.get();
        log.info("{} performing write (with metadata) at {}", clientId, dc.getName());
        Map<String, VersionVector> dependencies = dependencyMetadataFor(key);
        Duration delayOverride = consumeReplicationDelayOverride();
        CausalMetadata metadata = dc.applyWrite(key, value, dependencyVector.copy(), dependencies, delayOverride);
        metricsCollector.recordWrite(dc.getName());

        dependencyVector.merge(metadata.versionVector());
        recordSeenKey(key, metadata);
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
        log.info("{} {} read at {}: key={}, value={}", clientId, policy, dc.getName(), key, value);
        return value;
    }

    public void resetDependencies() {
        dependencyVector.clear();
        dependencyVersions.clear();
    }

    /** Manually register that {@code key} has the given version for future dependencies. */
    public void addDependency(String key, VersionVector versionVector) {
        if (key == null || versionVector == null) return;
        dependencyVersions.put(key, versionVector.copy());
    }

    /** Declare that the write depends on everything embedded in {@code metadata}. */
    public void addDependency(CausalMetadata metadata) {
        if (metadata == null) return;
        String metaKey = metadata.key();
        if (metaKey != null) {
            dependencyVersions.put(metaKey, metadata.versionVector());
        }
        metadata.dependencies().forEach((key, vector) -> dependencyVersions.put(key, vector.copy()));
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
            dependencyVersions.put(observedKey, metadata.versionVector());
        }
        metadata.dependencies().forEach((depKey, vector) -> dependencyVersions.put(depKey, vector.copy()));
    }

    private Map<String, VersionVector> dependencyMetadataFor(String key) {
        if (dependencyVersions.isEmpty()) {
            return Map.of();
        }
        Map<String, VersionVector> dependencies = new LinkedHashMap<>();
        for (Map.Entry<String, VersionVector> entry : dependencyVersions.entrySet()) {
            if (!entry.getKey().equals(key)) {
                dependencies.put(entry.getKey(), entry.getValue().copy());
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
