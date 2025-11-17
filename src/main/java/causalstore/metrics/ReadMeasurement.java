package causalstore.metrics;

import java.time.Instant;

public record ReadMeasurement(Instant timestamp,
                              String config,
                              int trial,
                              String key,
                              String caseName,
                              String policy,
                              String value,
                              long latencyMillis) {
}
