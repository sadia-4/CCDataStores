package causalstore.core;

import java.time.Instant;
import java.util.Objects;

public final class CausalMetadata {
    private final String origin;
    private final VersionVector versionVector;
    private final Instant timestamp;

    public CausalMetadata(String origin, VersionVector sourceVector) {
        this.origin = Objects.requireNonNull(origin, "origin");
        this.versionVector = sourceVector.copy();
        this.timestamp = Instant.now();
    }

    public String origin() {
        return origin;
    }

    public VersionVector versionVector() {
        return versionVector.copy();
    }

    public Instant timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "CausalMetadata{" +
                "origin='" + origin + '\'' +
                ", vector=" + versionVector +
                ", timestamp=" + timestamp +
                '}';
    }
}
