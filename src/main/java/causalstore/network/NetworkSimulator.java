package causalstore.network;

import causalstore.datacenter.DataCenter;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class NetworkSimulator {
    private final Duration baseDelay;

    public NetworkSimulator(Duration baseDelay) {
        this.baseDelay = baseDelay;
    }

    public Mono<Duration> delayFor(DataCenter source, DataCenter target) {
        long jitter = ThreadLocalRandom.current().nextLong(0,100);
        Duration baseNetwork = baseDelay.plusMillis(Math.abs(source.getNetworkDelayMs() - target.getNetworkDelayMs()));
        Duration totalDelay = baseNetwork.plusMillis(jitter);
        return Mono.delay(totalDelay).map(ignore -> totalDelay);
    }
}
