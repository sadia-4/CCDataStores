package causalstore.datacenter;

import causalstore.core.KeyValueStore;
import causalstore.core.VersionVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCenter {
    private static final Logger log = LoggerFactory.getLogger(DataCenter.class);

    private final String name;
    private final KeyValueStore store = new KeyValueStore();
    private final VersionVector versionVector = new VersionVector();
    private final int networkDelayMs;

    public DataCenter(String name, int delayMs) {
        this.name = name;
        this.networkDelayMs = delayMs;
    }

    public void applyWrite(String key, String value) {
        versionVector.increment(name);
        store.put(key, value);
        log.info("{} applied write: key={}, version={}", name, key, versionVector);
    }

    public String read(String key) {
        return store.get(key);
    }

    public int getNetworkDelayMs() {
        return networkDelayMs;
    }

    public String getName() {
        return name;
    }

    public VersionVector getVersionVector() {
        return versionVector;
    }
}
