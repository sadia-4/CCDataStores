package causalstore.core;

public interface CausalStoreNode {
    CausalMetadata applyWrite(String key, String value);
    void applyReplica(String key, String value, CausalMetadata metadata);
    String read(String key);
    String getName();
    VersionVector versionVector();
}
