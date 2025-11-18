package causalstore.core;

import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {
    public static final class Entry {
        private final String value;
        private final CausalMetadata metadata;

        private Entry(String value, CausalMetadata metadata) {
            this.value = value;
            this.metadata = metadata;
        }

        public String value() {
            return value;
        }

        public CausalMetadata metadata() {
            return metadata;
        }
    }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    public void put(String key, String value, CausalMetadata metadata) {
        store.put(key, new Entry(value, metadata));
    }

    public Entry get(String key) {
        return store.get(key);
    }

    public boolean containsKey(String key) {
        Entry entry = store.get(key);
        return entry != null && entry.value != null;
    }

    public boolean hasVersionAtLeast(String key, VersionVector required) {
        Entry entry = store.get(key);
        if (entry == null || entry.metadata == null || required == null) {
            return false;
        }
        return entry.metadata.versionVector().dominates(required);
    }
}
