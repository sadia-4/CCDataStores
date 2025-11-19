package causalstore.core;

import java.time.Duration;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class CausalMetadata {
    private final String origin;
    private final String key;
    private final String value;
    private final VersionVector versionVector;
    private final Instant timestamp;
    private final Map<String, Long> dependencies;
    private final Duration replicationDelayOverride;
    private final long globalSequence;

    public CausalMetadata(String origin,
                          String key,
                          String value,
                          VersionVector sourceVector,
                          Map<String, Long> dependencies,
                          Duration replicationDelayOverride,
                          long globalSequence) {
        this.origin = Objects.requireNonNull(origin, "origin");
        this.key = key;
        this.value = value;
        this.versionVector = sourceVector.copy();
        this.timestamp = Instant.now();
        if (dependencies == null || dependencies.isEmpty()) {
            this.dependencies = Collections.emptyMap();
        } else {
            Map<String, Long> working = new LinkedHashMap<>();
            working.putAll(dependencies);
            this.dependencies = Collections.unmodifiableMap(working);
        }
        this.replicationDelayOverride = replicationDelayOverride;
        this.globalSequence = globalSequence;
    }

    public String origin() {
        return origin;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public VersionVector versionVector() {
        return versionVector.copy();
    }

    public Instant timestamp() {
        return timestamp;
    }

    public Map<String, Long> dependencies() {
        return dependencies;
    }

    public Duration replicationDelayOverride() {
        return replicationDelayOverride;
    }

    public long globalSequence() {
        return globalSequence;
    }

    @Override
    public String toString() {
        return "CausalMetadata{" +
                "origin='" + origin + '\'' +
                ", key='" + key + '\'' +
                ", vector=" + versionVector +
                ", dependencies=" + dependencies +
                ", globalSequence=" + globalSequence +
                ", timestamp=" + timestamp +
                '}';
    }
}
