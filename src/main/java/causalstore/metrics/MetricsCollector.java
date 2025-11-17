package causalstore.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class MetricsCollector {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final LongAdder writeCount = new LongAdder();
    private final LongAdder replicationCount = new LongAdder();
    private final ConcurrentHashMap<String, Duration> replicationLatencies = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> readCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> readLatencyNanos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> readLatencySamples = new ConcurrentHashMap<>();
    private final LongAdder causalWaitCount = new LongAdder();
    private final LongAdder causalWaitNanos = new LongAdder();
    private final LongAdder linearWaitCount = new LongAdder();
    private final LongAdder linearWaitNanos = new LongAdder();

    public void recordWrite(String origin) {
        writeCount.increment();
    }

    public void recordReplication(String target, Duration latency) {
        replicationCount.increment();
        replicationLatencies.put(target, latency);
    }

    public MetricsSnapshot snapshot() {
        Map<String, Long> latenciesMillis = new HashMap<>();
        replicationLatencies.forEach((node, duration) -> latenciesMillis.put(node, duration.toMillis()));
        return new MetricsSnapshot(writeCount.sum(),
                replicationCount.sum(),
                latenciesMillis,
                aggregateReadLatencies(),
                aggregateWaitLatencies());
    }

    public String snapshotJson() {
        return snapshotJson(snapshot());
    }

    public String snapshotJson(MetricsSnapshot snapshot) {
        try {
            return MAPPER.writeValueAsString(snapshot);
        } catch (JsonProcessingException e) {
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    public record MetricsSnapshot(long writeCount,
                                  long replicationCount,
                                  Map<String, Long> latencies,
                                  Map<String, Map<String, Long>> readLatencies,
                                  Map<String, Long> waitLatencies) {
    }

    public void recordRead(String policy, Duration latency) {
        readCounts.computeIfAbsent(policy, key -> new LongAdder()).increment();
        readLatencyNanos.computeIfAbsent(policy, key -> new LongAdder()).add(latency.toNanos());
        readLatencySamples.computeIfAbsent(policy, key -> new ConcurrentLinkedQueue<>()).add(latency.toNanos());
    }

    public void recordCausalWait(Duration latency) {
        causalWaitCount.increment();
        causalWaitNanos.add(latency.toNanos());
    }

    public void recordLinearWait(Duration latency) {
        linearWaitCount.increment();
        linearWaitNanos.add(latency.toNanos());
    }

    private Map<String, Map<String, Long>> aggregateReadLatencies() {
        Map<String, Map<String, Long>> aggregation = new HashMap<>();
        readCounts.forEach((policy, counter) -> {
            long count = counter.sum();
            if (count == 0) return;
            Map<String, Long> stats = new HashMap<>();
            LongAdder latencyAdder = readLatencyNanos.get(policy);
            if (latencyAdder != null) {
                long avg = latencyAdder.sum() / count;
                stats.put("avg_millis", TimeUnit.NANOSECONDS.toMillis(avg));
            }
            ConcurrentLinkedQueue<Long> samples = readLatencySamples.get(policy);
            if (samples != null && !samples.isEmpty()) {
                List<Long> sorted = new ArrayList<>(samples);
                Collections.sort(sorted);
                stats.put("p50_millis", percentile(sorted, 0.50));
                stats.put("p95_millis", percentile(sorted, 0.95));
            }
            aggregation.put(policy, stats);
        });
        return aggregation;
    }

    private Map<String, Long> aggregateWaitLatencies() {
        Map<String, Long> waits = new HashMap<>();
        addWaitStats(waits, "causal-wait", causalWaitCount, causalWaitNanos);
        addWaitStats(waits, "linearizable-wait", linearWaitCount, linearWaitNanos);
        return waits;
    }

    private void addWaitStats(Map<String, Long> map, String label, LongAdder count, LongAdder nanos) {
        long calls = count.sum();
        if (calls == 0) return;
        long avg = nanos.sum() / calls;
        map.put(label, TimeUnit.NANOSECONDS.toMillis(avg));
    }

    private long percentile(List<Long> sortedSamples, double quantile) {
        if (sortedSamples.isEmpty()) return 0;
        int idx = (int) Math.ceil(quantile * sortedSamples.size()) - 1;
        idx = Math.max(0, Math.min(sortedSamples.size() - 1, idx));
        return TimeUnit.NANOSECONDS.toMillis(sortedSamples.get(idx));
    }
}
