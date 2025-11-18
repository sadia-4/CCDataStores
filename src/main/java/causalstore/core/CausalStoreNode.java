package causalstore.core;

import java.util.Map;

public interface CausalStoreNode {
    CausalMetadata applyWrite(String key, String value,
                              VersionVector dependencies,
                              Map<String, VersionVector> dependencyKeys);

    void applyReplica(String key, String value, CausalMetadata metadata);

    String read(String key);

    ReadResult readWithMetadata(String key);

    String getName();

    VersionVector versionVector();
}
