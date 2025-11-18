package causalstore.core;

public final class ReadResult {
    private final String value;
    private final CausalMetadata metadata;

    public ReadResult(String value, CausalMetadata metadata) {
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
