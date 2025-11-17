package causalstore.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class MetricsExporter implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STATS_HEADER = "timestamp,config,trial,writeCount,replicationCount,replicationLatencies,readLatencies,waitLatencies";
    private static final String READ_HEADER = "timestamp,config,trial,key,case,policy,value,latencyMillis";

    private final BufferedWriter jsonWriter;
    private final BufferedWriter csvWriter;
    private final BufferedWriter readWriter;

    public MetricsExporter(Path outputDirectory) {
        try {
            Files.createDirectories(outputDirectory);
            Path jsonPath = outputDirectory.resolve("metrics.jsonl");
            Path csvPath = outputDirectory.resolve("metrics.csv");
            Path readPath = outputDirectory.resolve("reads.csv");

            jsonWriter = Files.newBufferedWriter(jsonPath,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            csvWriter = Files.newBufferedWriter(csvPath,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            readWriter = Files.newBufferedWriter(readPath,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            if (Files.size(csvPath) == 0) {
                csvWriter.write(STATS_HEADER);
                csvWriter.newLine();
                csvWriter.flush();
            }
            if (Files.size(readPath) == 0) {
                readWriter.write(READ_HEADER);
                readWriter.newLine();
                readWriter.flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to initialize metrics exporter", e);
        }
    }

    public synchronized void export(MetricsCollector.MetricsSnapshot snapshot, String config, int trial) {
        String timestamp = Instant.now().toString();
        Map<String, Object> payload = new HashMap<>();
        payload.put("timestamp", timestamp);
        payload.put("snapshot", snapshot);

        try {
            jsonWriter.write(MAPPER.writeValueAsString(payload));
            jsonWriter.newLine();
            jsonWriter.flush();

            String latenciesJson = MAPPER.writeValueAsString(snapshot.latencies());
            String readJson = MAPPER.writeValueAsString(snapshot.readLatencies());
            String waitJson = MAPPER.writeValueAsString(snapshot.waitLatencies());
            String csvLine = String.join(",",
                    quote(timestamp),
                    quote(config),
                    String.valueOf(trial),
                    String.valueOf(snapshot.writeCount()),
                    String.valueOf(snapshot.replicationCount()),
                    quote(latenciesJson),
                    quote(readJson),
                    quote(waitJson));
            csvWriter.write(csvLine);
            csvWriter.newLine();
            csvWriter.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to export metrics", e);
        }
    }

    public synchronized void exportRead(ReadMeasurement measurement) {
        try {
            String csvLine = String.join(",",
                    quote(measurement.timestamp().toString()),
                    quote(measurement.config()),
                    String.valueOf(measurement.trial()),
                    quote(measurement.key()),
                    quote(measurement.caseName()),
                    quote(measurement.policy()),
                    quote(measurement.value()),
                    String.valueOf(measurement.latencyMillis()));
            readWriter.write(csvLine);
            readWriter.newLine();
            readWriter.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to export read measurement", e);
        }
    }

    private String quote(String value) {
        if (value == null) {
            return "\"\"";
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void close() {
        try {
            jsonWriter.close();
        } catch (IOException ignored) {
        }
        try {
            csvWriter.close();
        } catch (IOException ignored) {
        }
        try {
            readWriter.close();
        } catch (IOException ignored) {
        }
    }
}
