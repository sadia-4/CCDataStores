# CausalStore Experiment

This project simulates a causal key-value store with configurable WAN delays, explicit dependency tracking, and a linearizable baseline. Each write carries a `CausalMetadata` payload with vector clocks, a global sequence, and optional replication delay overrides. Experiments log latencies for , `CAUSAL`, and `LINEARIZABLE` reads in `results.csv`.

## Modules

- `client`: contains `ClientSession` that tracks dependencies, injects them into `CausalMetadata`, and exposes helpers for causal reads/writes plus optional delay overrides.  
- `datacenter`: defines `DataCenter`, replica storage, pending queues, and `ReplicationManager`, which applies writes after dependency checks.  
- `core`: includes metadata helpers (`CausalMetadata`, `GlobalVersionClock`, `VersionVector`, `ReadResult`) used by both clients and datacenters.  
- `network`: simulates WAN latency (`NetworkSimulator`) that `ReplicationManager` uses when delivering writes asynchronously.  
- `metrics`: logs latencies to `results.csv` and records replication stats.

## Getting started

1. Navigate to the repository root (where `gradlew` lives).
2. (Optional) If you are behind an HTTP proxy, add the following lines to `gradle.properties` (create the file next to `gradlew` if needed) before running Gradle:
   ```
   systemProp.http.proxyHost=your-proxy-host
   systemProp.http.proxyPort=your-proxy-port
   systemProp.https.proxyHost=your-proxy-host
   systemProp.https.proxyPort=your-proxy-port
   ```
3. Refresh dependencies and build:
   ```bash
   ./gradlew build
   ```

## Running the main experiment

- Run `./gradlew run` to execute `Main.main`. It logs latencies to `results.csv` and prints the trace to the console.
- Check `results.csv` for rows such as `exp1_causal` vs `exp2_linearizable` 


- Adjust the datacenter delays in `Main` (the `List<DataCenter>`) or use `ClientSession.setNextReplicationDelay` to simulate slow vs fast links.
- The linearizable reader is configured in `Main` via `LinearizableClientSession`. 
