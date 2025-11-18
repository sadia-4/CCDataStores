package causalstore.core;

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
    private final Map<String, VersionVector> dependencies;

    public CausalMetadata(String origin,
                          String key,
                          String value,
                          VersionVector sourceVector,
                          Map<String, VersionVector> dependencies) {
        this.origin = Objects.requireNonNull(origin, "origin");
        this.key = key;
        this.value = value;
        this.versionVector = sourceVector.copy();
        this.timestamp = Instant.now();
        if (dependencies == null || dependencies.isEmpty()) {
            this.dependencies = Collections.emptyMap();
        } else {
            Map<String, VersionVector> working = new LinkedHashMap<>();
            dependencies.forEach((k, vector) -> working.put(k, vector.copy()));
            this.dependencies = Collections.unmodifiableMap(working);
        }
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

    public Map<String, VersionVector> dependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return "CausalMetadata{" +
                "origin='" + origin + '\'' +
                ", key='" + key + '\'' +
                ", vector=" + versionVector +
                ", dependencies=" + dependencies +
                ", timestamp=" + timestamp +
                '}';
    }
}
