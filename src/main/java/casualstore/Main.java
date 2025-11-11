package causalstore;

import causalstore.datacenter.DataCenter;
import causalstore.client.ClientSession;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting casually_consistent_stores or-based Causal Store Simulation...");

        // Create datacenters
        DataCenter dc1 = new DataCenter("DC1", 5);
        DataCenter dc2 = new DataCenter("DC2", 10);
        DataCenter dc3 = new DataCenter("DC3", 15);

        List<DataCenter> datacenters = List.of(dc1, dc2, dc3);

        // Start simulated clients
        // Flux.interval(Duration.ofMillis(200))
        //         .publishOn(Schedulers.parallel())
        //         .subscribe(i -> {
        //             DataCenter nearest = datacenters.get((int) (i % 3));
        //             ClientSession client = new ClientSession("Client-" + i, nearest);
        //             client.performWrite("post" + i, "data" + i);
        //         });
        DataCenter nearest = datacenters.get(0); // choose which datacenter
        
        ClientSession client = new ClientSession("Client-Manual", nearest);
        client.performWrite("key20", "sadiya");

        // Keep running simulation
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
