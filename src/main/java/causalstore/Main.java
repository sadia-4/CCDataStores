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
import causalstore.metrics.CSVLogger;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // public static void main(String[] args) throws InterruptedException {
    //     CausalReadPolicy readPolicy = args.length > 0 ? CausalReadPolicy.valueOf(args[0].toUpperCase()) : CausalReadPolicy.EVENTUAL;
    //     log.info("Starting simple scenario with read policy {}", readPolicy);

    //     MetricsCollector metricsCollector = new MetricsCollector();
    //     List<DataCenter> datacenters = List.of(
    //             new DataCenter("DC-A", 5),
    //             new DataCenter("DC-B", 1000),
    //             new DataCenter("DC-C", 15)
    //     );
    //     ReplicationManager replicationManager = new ReplicationManager(datacenters,
    //             new NetworkSimulator(Duration.ofMillis(30)), metricsCollector, Duration.ZERO);

    //     Map<String, DataCenter> manualAssignments = new HashMap<>();
    //     manualAssignments.put("Writer1", datacenters.get(0));
    //     manualAssignments.put("Writer2", datacenters.get(2));
    //     manualAssignments.put("Reader", datacenters.get(2));
    //     manualAssignments.put("Reader2", datacenters.get(2));

    //     ClientSession writer1 = new ClientSession("Writer1", () -> manualAssignments.get("Writer1"),
    //             replicationManager, metricsCollector);
    //     ClientSession writer2 = new ClientSession("Writer2", () -> manualAssignments.get("Writer2"),
    //     replicationManager, metricsCollector);
    //     ClientSession reader = new ClientSession("Reader", () -> manualAssignments.get("Reader"),
    //             replicationManager, metricsCollector);
    //             ClientSession reader2 = new ClientSession("Reader2", () -> manualAssignments.get("Reader2"),
    //             replicationManager, metricsCollector);
    //     LinearizableClientSession linearReader = new LinearizableClientSession("LinearClient",
    //             datacenters.get(1), replicationManager, metricsCollector);

    //     writer1.performWrite("discover", "value-alpha");
    //     Thread.sleep(1000);
    //     writer2.performWrite("discover", "value-update");
    //     Thread.sleep(100);

    //     readWithPolicy(reader, "discover", readPolicy);
    //     readWithPolicy(reader2, "discover", readPolicy);
    //     Thread.sleep(10000);
    //     readLinearizable(linearReader, "discover");
    //     readLinearizable(linearReader, "discover");

    //     log.info("Simple scenario completed. Metrics: {}", metricsCollector.snapshotJson());
    //     replicationManager.shutdown();
    // }

    // private static void readWithPolicy(ClientSession client, String key, CausalReadPolicy policy) {
    //     long start = System.nanoTime();
    //     String value = client.read(key, policy);
    //     long latency = Duration.ofNanos(System.nanoTime() - start).toMillis();
    //     log.info("{} {} read {} -> {} ({} ms)", client.getClass().getSimpleName(), policy, key, value, latency);
    // }

    // private static void readLinearizable(LinearizableClientSession client, String key) {
    //     long start = System.nanoTime();
    //     String value = client.readLinearizable(key);
    //     long latency = Duration.ofNanos(System.nanoTime() - start).toMillis();
    //     log.info("Linearizable read {} -> {} ({} ms)", key, value, latency);
    // }
    public static void main(String[] args) throws InterruptedException {

    CSVLogger csv = new CSVLogger("results.csv");

    MetricsCollector metricsCollector = new MetricsCollector();
    List<DataCenter> datacenters = List.of(
            new DataCenter("DC-0", 5),
            new DataCenter("DC-1", 100),
            new DataCenter("DC-2", 15)
    );

    ReplicationManager replicationManager = new ReplicationManager(
            datacenters,
            new NetworkSimulator(Duration.ofMillis(30)),
            metricsCollector,
            Duration.ZERO
    );

    // Assignments
    Map<String, DataCenter> manual = Map.of(
            "Writer1", datacenters.get(1),
            "Writer2", datacenters.get(0),
            " Reader1", datacenters.get(1),
            "Reader2", datacenters.get(2)
    );

    // Sessions
    ClientSession Writer1 = new ClientSession("Writer1", () -> manual.get("Writer1"), replicationManager, metricsCollector);
    ClientSession Writer2 = new ClientSession("Writer2", () -> manual.get("Writer2"), replicationManager, metricsCollector);
    ClientSession Reader2 = new ClientSession("Reader2", () -> manual.get("Reader2"), replicationManager, metricsCollector);
   
    ClientSession  Reader1 = new ClientSession(" Reader1", () -> manual.get(" Reader1"), replicationManager, metricsCollector);
    LinearizableClientSession linearReader = new LinearizableClientSession("Linearizable", datacenters.get(1), replicationManager, metricsCollector);

    // -----------------------------------------------------
    //  EXP 1: Local read latency (Eventual)
    // -----------------------------------------------------
    // Writer1.performWrite("k1", "alpha");
    // Thread.sleep(100);

    // long t1 = System.nanoTime();
    // String v1 =  Reader1.read("k1", CausalReadPolicy.EVENTUAL);
    // long lat1 = Duration.ofNanos(System.nanoTime() - t1).toMillis();

    // csv.log("exp1_local_eventual", "read", "eventual", "DC-C", lat1, v1);


    // -----------------------------------------------------
    //  EXP 3: k2 depends on k1 (causal dependency)
    // -----------------------------------------------------
    CausalMetadata metaK1 = Writer1.performWriteWithMetadata("k1", "alpha");
    
   Writer2.addDependency("k1", metaK1.versionVector());
    
    Writer2.performWrite("k2", "beta");
 Thread.sleep(30);
    long t2 = System.nanoTime();
    String v2 = Reader2.read("k2", CausalReadPolicy.EVENTUAL);
    long lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_eventual-before", "read", "eventual", "DC-B", lat2, v2);
    
    t2 = System.nanoTime();
    v2 = Reader2.read("k2", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_causal", "read", "causal", "DC-B", lat2, v2);

    t2 = System.nanoTime();
    v2 = Reader2.read("k2", CausalReadPolicy.EVENTUAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_eventual-after", "read", "eventual", "DC-B", lat2, v2);

    t2 = System.nanoTime();
    v2 = Reader2.read("k1", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_causal", "read", "causal", "DC-B", lat2, v2);

    t2 = System.nanoTime();
    v2 = Reader2.read("k1", CausalReadPolicy.EVENTUAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_eventual-after", "read", "eventual", "DC-B", lat2, v2);
 t2 = System.nanoTime();
     v2 = Reader1.read("k2", CausalReadPolicy.EVENTUAL);
     lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp3_eventual-before", "read", "eventual", "DC-B", lat2, v2);
    
    // // -----------------------------------------------------
    // //  EXP 3: Linearizable read latency
    // // -----------------------------------------------------
    // Writer2.performWrite("k3", "x1");
    // Thread.sleep(50);

    // long t3 = System.nanoTime();
    // String v3 = linearReader.readLinearizable("k3");
    // long lat3 = Duration.ofNanos(System.nanoTime() - t3).toMillis();

    // csv.log("exp3_linearizable", "read", "linearizable", "DC-B", lat3, v3);


    // // -----------------------------------------------------
    // //  EXP 4: Throughput under load
    // // -----------------------------------------------------
    // for (int i = 0; i < 10; i++) {
    //     String key = "load" + i;
    //     Writer1.performWrite(key, "v" + i);

    //     long start = System.nanoTime();
    //     String val =  Reader1.read(key, CausalReadPolicy.EVENTUAL);
    //     long latency = Duration.ofNanos(System.nanoTime() - start).toMillis();
    //     csv.log("exp4_load_eventual", "read", "eventual", "DC-C", latency, val);

    //     try {
    //         Thread.sleep(100);
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //     }

    //     long startC = System.nanoTime();
    //     String causalVal =  Reader1.read(key, CausalReadPolicy.CAUSAL);
    //     long causalLatency = Duration.ofNanos(System.nanoTime() - startC).toMillis();
    //     csv.log("exp4_load_causal", "read", "causal", "DC-C", causalLatency, causalVal);
    // }

    replicationManager.shutdown();
}

}
