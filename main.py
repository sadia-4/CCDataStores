import asyncio
import random
from statistics import mean

from client.client import ClientSession
from datacenters.datacenter import DataCenter


def connect(a: DataCenter, b: DataCenter, latency_range_ms):
    a.add_peer(b, latency_range_ms)
    b.add_peer(a, latency_range_ms)


def build_topology():
    dc_us = DataCenter("us-east", local_latency_ms=(3, 7))
    dc_eu = DataCenter("eu-west", local_latency_ms=(5, 9))
    dc_as = DataCenter("asia-south", local_latency_ms=(7, 12))

    connect(dc_us, dc_eu, (80, 100))
    connect(dc_us, dc_as, (140, 180))
    connect(dc_eu, dc_as, (120, 150))
    return [dc_us, dc_eu, dc_as]


async def run_client_workload(client, peers, iterations, consistency):
    neighbor_names = [p.name for p in peers if p.name != client.name]
    if not neighbor_names:
        neighbor_names = [client.name]

    for i in range(iterations):
        post_key = f"feed:{client.name}"
        await client.put(post_key, f"{client.name}-post-{i}", consistency=consistency)

        neighbor = random.choice(neighbor_names)
        await client.get(f"feed:{neighbor}", consistency=consistency)

        if i % 3 == 0:
            await client.put("doc:shared", f"{client.name}-edit-{i}", consistency=consistency)
        else:
            await client.get("doc:shared", consistency=consistency)


async def run_experiment(label, clients, datacenters, iterations, consistency):
    print(f"\n=== {label} ({consistency.upper()}) ===")
    for dc in datacenters:
        dc.reset_metrics()
    for client in clients:
        client.reset_metrics()

    await asyncio.gather(
        *(run_client_workload(client, clients, iterations, consistency) for client in clients)
    )
    report_metrics(datacenters)
    report_client_metrics(clients)


def report_metrics(datacenters):
    for dc in datacenters:
        print(f"\n{dc.dc_id}")
        for metric_name, samples in dc.metrics.items():
            if not samples:
                continue
            avg_ms = mean(samples) * 1000
            print(f"  {metric_name:<22} avg={avg_ms:7.2f}ms  samples={len(samples)}")


def report_client_metrics(clients):
    print("\nClient-observed latencies")
    for client in clients:
        print(f"  {client.name}")
        for metric_name, samples in client.metrics.items():
            if not samples:
                continue
            avg_ms = mean(samples) * 1000
            print(f"    {metric_name:<22} avg={avg_ms:7.2f}ms  samples={len(samples)}")


async def main():
    datacenters = build_topology()
    dc_map = {dc.dc_id: dc for dc in datacenters}

    causal_clients = [
        ClientSession("alice", dc_map["us-east"]),
        ClientSession("bruno", dc_map["eu-west"]),
        ClientSession("chen", dc_map["asia-south"]),
    ]

    leader = dc_map["us-east"]
    linearizable_clients = [
        ClientSession("alice", dc_map["us-east"], leader_dc=leader),
        ClientSession("bruno", dc_map["eu-west"], leader_dc=leader),
        ClientSession("chen", dc_map["asia-south"], leader_dc=leader),
    ]

    await run_experiment(
        "Local causal feeds & collaborative editing", causal_clients, datacenters, iterations=6, consistency="causal"
    )
    await run_experiment(
        "Linearizable baseline (global leader)", linearizable_clients, datacenters, iterations=6, consistency="linearizable"
    )


if __name__ == "__main__":
    asyncio.run(main())
