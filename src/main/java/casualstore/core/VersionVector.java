package causalstore.core;

import java.util.HashMap;
import java.util.Map;

public class VersionVector {
    private final Map<String, Integer> versions = new HashMap<>();

    public void increment(String nodeId) {
        versions.put(nodeId, versions.getOrDefault(nodeId, 0) + 1);
    }

    public boolean isCausallyBefore(VersionVector other) {
        for (var entry : versions.entrySet()) {
            int otherVersion = other.versions.getOrDefault(entry.getKey(), 0);
            if (entry.getValue() > otherVersion) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return versions.toString();
    }
}
