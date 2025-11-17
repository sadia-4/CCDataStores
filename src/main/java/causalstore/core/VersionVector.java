package causalstore.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VersionVector {
    private final Map<String, Integer> versions = new ConcurrentHashMap<>();

    public void increment(String nodeId) {
        versions.put(nodeId, versions.getOrDefault(nodeId, 0) + 1);
    }

    public void merge(VersionVector other) {
        other.versions.forEach((node, version) ->
                versions.merge(node, version, Math::max));
    }

    public VersionVector copy() {
        VersionVector copy = new VersionVector();
        versions.forEach(copy.versions::put);
        return copy;
    }

    public void clear() {
        versions.clear();
    }

    public Map<String, Integer> snapshot() {
        return new HashMap<>(versions);
    }

    public boolean isCausallyBefore(VersionVector other) {
        for (Map.Entry<String, Integer> entry : versions.entrySet()) {
            int otherVersion = other.versions.getOrDefault(entry.getKey(), 0);
            if (entry.getValue() > otherVersion) return false;
        }
        return true;
    }

    public boolean dominates(VersionVector other) {
        for (Map.Entry<String, Integer> entry : other.versions.entrySet()) {
            int otherVersion = entry.getValue();
            int localVersion = versions.getOrDefault(entry.getKey(), 0);
            if (localVersion < otherVersion) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return versions.toString();
    }
}
