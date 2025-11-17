package causalstore.metrics;

import java.io.FileWriter;
import java.io.IOException;

public class CSVLogger {

    private final String file;

    public CSVLogger(String file) {
        this.file = file;
        try (FileWriter fw = new FileWriter(file, false)) {
            fw.write("experiment,operation,policy,dc,latency_ms,value\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void log(String experiment, String operation, String policy,
                                 String dc, long latencyMs, String value) {
        try (FileWriter fw = new FileWriter(file, true)) {
            fw.write(String.format("%s,%s,%s,%s,%d,%s\n",
                    experiment, operation, policy, dc, latencyMs, value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
