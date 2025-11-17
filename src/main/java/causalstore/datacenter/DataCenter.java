package causalstore.datacenter;

import causalstore.core.CausalMetadata;
import causalstore.core.CausalStoreNode;
import causalstore.core.KeyValueStore;
import causalstore.core.VersionVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DataCenter implements CausalStoreNode {
    private static final Logger log = LoggerFactory.getLogger(DataCenter.class);

    private final String name;
    private final VersionVector versionVector = new VersionVector();
    private final int networkDelayMs;
    private final List<KeyValueStore> replicas;
    private final AtomicInteger readIndex = new AtomicInteger();
    private final Queue<PendingReplication> pendingReplicas = new ConcurrentLinkedQueue<>();
    private final Sinks.Many<ApplyEvent> changeSink = Sinks.many().multicast().onBackpressureBuffer();

    public DataCenter(String name, int delayMs) {
        this(name, delayMs, 3);
    }

    public DataCenter(String name, int delayMs, int replicaCount) {
        this.name = name;
        this.networkDelayMs = delayMs;
        if (replicaCount <= 0) throw new IllegalArgumentException("replicaCount must be >= 1");
        List<KeyValueStore> list = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            list.add(new KeyValueStore());
        }
        this.replicas = Collections.unmodifiableList(list);
    }

    @Override
    public CausalMetadata applyWrite(String key, String value) {
        versionVector.increment(name);
        writeToAllReplicas(key, value);
        CausalMetadata metadata = new CausalMetadata(name, versionVector);
        log.info("{} applied write: key={}, replicated to {} local replicas, metadata={}",
                name, key, replicas.size(), metadata);
        drainPending();
        changeSink.tryEmitNext(new ApplyEvent(key, metadata, ApplyType.LOCAL_WRITE));
        return metadata;
    }

    @Override
    public void applyReplica(String key, String value, CausalMetadata metadata) {
        PendingReplication incoming = new PendingReplication(key, value, metadata);
        if (canApply(metadata)) {
            applyReplicaNow(incoming);
            drainPending();
        } else {
            pendingReplicas.add(incoming);
            log.debug("{} buffered replication for key={} because dependencies are missing (metadata={})",
                    name, key, metadata);
        }
    }

    @Override
    public String read(String key) {
        int idx = Math.floorMod(readIndex.getAndIncrement(), replicas.size());
        return replicas.get(idx).get(key);
    }

    public int getNetworkDelayMs() {
        return networkDelayMs;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public VersionVector versionVector() {
        return versionVector.copy();
    }

    public int replicaCount() {
        return replicas.size();
    }

    private void drainPending() {
        if (pendingReplicas.isEmpty()) return;

        boolean progress;
        do {
            progress = false;
            List<PendingReplication> deferred = new ArrayList<>();
            PendingReplication pending = pendingReplicas.poll();
            while (pending != null) {
                if (canApply(pending.metadata)) {
                    applyReplicaNow(pending);
                    progress = true;
                } else {
                    deferred.add(pending);
                }
                pending = pendingReplicas.poll();
            }
            pendingReplicas.addAll(deferred);
        } while (progress && !pendingReplicas.isEmpty());
    }

    private void applyReplicaNow(PendingReplication pending) {
        writeToAllReplicas(pending.key, pending.value);
        versionVector.merge(pending.metadata.versionVector());
        log.info("{} applied replicated write: key={}, from={}, applied to {} local replicas, metadata={}",
                name, pending.key, pending.metadata.origin(), replicas.size(), pending.metadata);
        changeSink.tryEmitNext(new ApplyEvent(pending.key, pending.metadata, ApplyType.REPLICA_APPLIED));
    }

    private void writeToAllReplicas(String key, String value) {
        for (KeyValueStore store : replicas) {
            store.put(key, value);
        }
    }

    private boolean canApply(CausalMetadata metadata) {
        Map<String, Integer> incoming = metadata.versionVector().snapshot();
        Map<String, Integer> local = versionVector.snapshot();
        String origin = metadata.origin();
        for (Map.Entry<String, Integer> entry : incoming.entrySet()) {
            String node = entry.getKey();
            int requiredVersion = entry.getValue();
            int localVersion = local.getOrDefault(node, 0);
            if (node.equals(origin)) {
                if (localVersion + 1 != requiredVersion) {
                    return false;
                }
            } else if (localVersion < requiredVersion) {
                return false;
            }
        }
        return true;
    }

    private record PendingReplication(String key, String value, CausalMetadata metadata) {
    }

    public Flux<ApplyEvent> changes() {
        return changeSink.asFlux();
    }

    public enum ApplyType { LOCAL_WRITE, REPLICA_APPLIED }

    public record ApplyEvent(String key, CausalMetadata metadata, ApplyType type) {}
}
