package causalstore.datacenter;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class NearestDataCenterSelector {
    private final List<DataCenter> dataCenters;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public NearestDataCenterSelector(List<DataCenter> dataCenters) {
        this.dataCenters = List.copyOf(dataCenters);
    }

    public DataCenter nearest(String clientRegionHint) {
        Objects.requireNonNull(clientRegionHint, "clientRegionHint");
        return dataCenters.stream()
                .min(Comparator.comparingInt(dc -> estimatedLatency(clientRegionHint, dc)))
                .orElseThrow(() -> new IllegalStateException("no data centers available"));
    }

    public DataCenter defaultNearest() {
        return dataCenters.stream()
                .min(Comparator.comparingInt(DataCenter::getNetworkDelayMs))
                .orElseThrow(() -> new IllegalStateException("no data centers available"));
    }

    public DataCenter randomNearest(String clientRegionHint) {
        Objects.requireNonNull(clientRegionHint, "clientRegionHint");
        double totalWeight = 0.0;
        double[] prefix = new double[dataCenters.size()];
        for (int i = 0; i < dataCenters.size(); i++) {
            int latency = estimatedLatency(clientRegionHint, dataCenters.get(i));
            double weight = weightForLatency(latency);
            totalWeight += weight;
            prefix[i] = totalWeight;
        }
        double r = random.nextDouble() * totalWeight;
        for (int i = 0; i < prefix.length; i++) {
            if (r <= prefix[i]) {
                return dataCenters.get(i);
            }
        }
        return dataCenters.get(dataCenters.size() - 1);
    }

    private double weightForLatency(int latency) {
        int capped = Math.min(latency, 2000); // remove extreme differences
        return 1.0 / (1.0 + capped);
    }

    private int estimatedLatency(String clientRegionHint, DataCenter candidate) {
        int base = candidate.getNetworkDelayMs();
        if (candidate.getName().equalsIgnoreCase(clientRegionHint)) {
            return base;
        }
        return base + 15;
    }
}
