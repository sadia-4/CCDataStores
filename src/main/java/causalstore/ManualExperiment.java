package causalstore;

import causalstore.client.ClientSession;
import causalstore.client.LinearizableClientSession;
import causalstore.core.CausalMetadata;
import causalstore.core.CausalReadPolicy;
import causalstore.datacenter.DataCenter;
import causalstore.datacenter.ReplicationManager;
import causalstore.metrics.MetricsCollector;
import causalstore.network.NetworkSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ManualExperiment {
    private static final Logger log = LoggerFactory.getLogger(ManualExperiment.class);

    public static void main(String[] args) throws InterruptedException {
        CausalReadPolicy policy = args.length > 0 ? CausalReadPolicy.valueOf(args[0].toUpperCase()) : CausalReadPolicy.EVENTUAL;
        log.info("Running manual scenario with policy {}", policy);
        runScenario(policy);
    }

    private static void runScenario(CausalReadPolicy policy) throws InterruptedException {
        MetricsCollector metricsCollector = new MetricsCollector();
        List<DataCenter> datacenters = List.of(
                new DataCenter("DC-A", 5),
                new DataCenter("DC-B", 10),
                new DataCenter("DC-C", 15)
        );
        ReplicationManager replicationManager = new ReplicationManager(datacenters,
                new NetworkSimulator(Duration.ofMillis(50)), metricsCollector, Duration.ZERO);

        Map<String, DataCenter> assignments = new HashMap<>();
        assignments.put("Client-0", datacenters.get(0));
        assignments.put("Client-1", datacenters.get(1));
        assignments.put("Client-2", datacenters.get(1));
        assignments.put("Client-3", datacenters.get(2));

        ClientSession writer = new ClientSession("Client-0", () -> assignments.get("Client-0"), replicationManager, metricsCollector);
        ClientSession reader = new ClientSession("Client-1", () -> assignments.get("Client-1"), replicationManager, metricsCollector);
        LinearizableClientSession linearReader = new LinearizableClientSession("LinearClient",
                datacenters.get(0), replicationManager, metricsCollector);

        String key = "post-42";
        String value = "value-" + ThreadLocalRandom.current().nextInt(1, 100);
        CausalMetadata meta = writer.performWrite(key, value);
        log.info("Writer stored {} -> {}", key, value);

        Thread.sleep(100);

        switch (policy) {
            case EVENTUAL -> readWithPolicy(reader, key, CausalReadPolicy.EVENTUAL);
            case CAUSAL -> readWithPolicy(reader, key, CausalReadPolicy.CAUSAL);
            case LINEARIZABLE -> readLinearizable(linearReader, key);
        }

        Thread.sleep(500);
        readWithPolicy(reader, key, CausalReadPolicy.EVENTUAL);
        if (policy != CausalReadPolicy.LINEARIZABLE) {
            readWithPolicy(reader, key, CausalReadPolicy.CAUSAL);
        }

        replicationManager.shutdown();
    }

    private static void readWithPolicy(ClientSession client,
                                       String key,
                                       CausalReadPolicy policy) {
        long start = System.nanoTime();
        String value = client.read(key, policy);
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        log.info("{} read [{}] -> {} ({} ms)", client.getClass().getSimpleName(), policy, value, latency);
    }

    private static void readLinearizable(LinearizableClientSession client, String key) {
        long start = System.nanoTime();
        String value = client.readLinearizable(key);
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        log.info("linearizable read -> {} ({} ms)", value, latency);
    }
}
