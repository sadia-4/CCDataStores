package causalstore.client;

import causalstore.datacenter.DataCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSession {
    private static final Logger log = LoggerFactory.getLogger(ClientSession.class);
    private final String clientId;
    private final DataCenter localDC;

    public ClientSession(String id, DataCenter dc) {
        this.clientId = id;
        this.localDC = dc;
    }

    public void performWrite(String key, String value) {
        log.info("{} performing write at {}", clientId, localDC.getName());
        localDC.applyWrite(key, value);
    }
}
