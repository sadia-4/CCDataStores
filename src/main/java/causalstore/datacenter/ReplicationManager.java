package causalstore.datacenter;

import causalstore.core.CausalMetadata;
import causalstore.metrics.MetricsCollector;
import causalstore.network.NetworkSimulator;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final List<DataCenter> dataCenters;
    private final NetworkSimulator networkSimulator;
    private final MetricsCollector metricsCollector;
    private final Duration replicationInterval;
    private final Queue<ReplicationRequest> pending = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler;

    public ReplicationManager(List<DataCenter> dataCenters,
                              NetworkSimulator networkSimulator,
                              MetricsCollector metricsCollector,
                              Duration replicationInterval) {
        this.dataCenters = List.copyOf(dataCenters);
        this.networkSimulator = networkSimulator;
        this.metricsCollector = metricsCollector;
        this.replicationInterval = replicationInterval;
        if (replicationInterval != null && !replicationInterval.isZero()) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::flushPending,
                    replicationInterval.toMillis(), replicationInterval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
        scheduler = null;
        }
    }

    public void replicate(String key, String value, DataCenter source, CausalMetadata metadata) {
        ReplicationRequest request = new ReplicationRequest(key, value, source, metadata);
        if (scheduler == null) {
            dispatch(request);
        } else {
            pending.add(request);
        }
    }

    private void flushPending() {
        ReplicationRequest request;
        while ((request = pending.poll()) != null) {
            dispatch(request);
        }
    }

    public void replicateSync(String key, String value, DataCenter source, CausalMetadata metadata) {
        for (DataCenter target : dataCenters) {
            if (target == source) continue;
            Duration delay = networkSimulator.delayFor(source, target).block();
            target.applyReplica(key, value, metadata);
            metricsCollector.recordReplication(target.getName(), delay);
            log.debug("Replicated (sync) key={} from {} to {} after {}ms", key, source.getName(), target.getName(), delay.toMillis());
        }
    }

    private void dispatch(ReplicationRequest request) {
        Flux.fromIterable(dataCenters)
                .filter(target -> target != request.source)
                .flatMap(target -> networkSimulator.delayFor(request.source, target)
                        .doOnNext(delay -> {
                            target.applyReplica(request.key, request.value, request.metadata);
                            metricsCollector.recordReplication(target.getName(), delay);
                            log.debug("Replicated key={} from {} to {} after {}ms", request.key, request.source.getName(), target.getName(), delay.toMillis());
                        }))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private record ReplicationRequest(String key, String value,
                                      DataCenter source,
                                      CausalMetadata metadata) {
    }

    public List<DataCenter> allDataCenters() {
        return dataCenters;
    }

    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
