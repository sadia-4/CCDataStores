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

    public static void main(String[] args) throws InterruptedException {

    CSVLogger csv = new CSVLogger("results.csv");

    MetricsCollector metricsCollector = new MetricsCollector();
    List<DataCenter> datacenters = List.of(
            new DataCenter("DC-0", 5),
            new DataCenter("DC-1", 10),
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
            "Writer2", datacenters.get(2),
            " Reader1", datacenters.get(1),//DC-1
            "Reader2", datacenters.get(2) //DC-2
    );

    // Sessions
    ClientSession Writer1 = new ClientSession("Writer1", () -> manual.get("Writer1"), replicationManager, metricsCollector);
    ClientSession Writer2 = new ClientSession("Writer2", () -> manual.get("Writer2"), replicationManager, metricsCollector);
    ClientSession Reader2 = new ClientSession("Reader2", () -> manual.get("Reader2"), replicationManager, metricsCollector);
   
    ClientSession  Reader1 = new ClientSession(" Reader1", () -> manual.get(" Reader1"), replicationManager, metricsCollector);
    LinearizableClientSession linearReader = new LinearizableClientSession("Linearizable", datacenters.get(1), 1, replicationManager, metricsCollector);

    // -----------------------------------------------------
    //  EXP 1: k2 depends on k1 (causal dependency)
    // -----------------------------------------------------
    Writer2.setNextReplicationDelay(Duration.ofMillis(1000));
    CausalMetadata metaX1 = Writer2.performWrite("x1", "val1");
    Writer2.addDependency("x1", metaX1.globalSequence());
    Writer2.setNextReplicationDelay(Duration.ZERO);
    Writer2.performWrite("x2", "beta");
    long  t2 = System.nanoTime();
    String v2 = Reader2.read("x2", CausalReadPolicy.CAUSAL);
    long  lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp1_causal", "reader2", "causal", "DC-2", lat2, "key-x2=" + v2);
    Thread.sleep(200);
    t2 = System.nanoTime();
    v2 = Reader2.read("x1", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp1_causal", "reader2", "causal", "DC-2", lat2, "key-x1=" + v2);
    t2 = System.nanoTime();
    v2 = Reader1.read("x2", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp1_causal", "reader1", "causal", "DC-1", lat2, "key-X2=" + v2);
    t2 = System.nanoTime();
    v2 = Reader1.read("x1", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp1_causal", "reader1", "causal", "DC-1", lat2, "key-x1=" + v2);

    // -----------------------------------------------------
    //  EXP 2: causal vs linearizable latency
    // -----------------------------------------------------
    Writer1.setNextReplicationDelay(Duration.ZERO);
    CausalMetadata metaK1 = Writer1.performWrite("k1", "alpha");
    Writer1.addDependency("k1", metaK1.globalSequence());
    Writer1.performWrite("k2", "beta");
    t2 = System.nanoTime();
   v2 = Reader1.read("k2", CausalReadPolicy.CAUSAL);
    lat2 = Duration.ofNanos(System.nanoTime() - t2).toMillis();
    csv.log("exp2_causal", "reader1", "causal", "DC-1", lat2, "key-k2=" + v2);

   long t3 = System.nanoTime();
   String v3 = linearReader.readLinearizable("k2");
   long  lat3 = Duration.ofNanos(System.nanoTime() - t3).toMillis();

   csv.log("exp2_causal", "linear read", "causal", "DC-1", lat3, "key-k2=" + v3);


// t3 = System.nanoTime();
//    v3 = linearReader2.readLinearizable("k3");
//     lat3 = Duration.ofNanos(System.nanoTime() - t3).toMillis();

//     csv.log("exp2_linearizable", "reader2", "linearizable", "DC-2", lat3, v3);

   
}

}
