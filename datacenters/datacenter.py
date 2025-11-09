import asyncio
import random
import time
from typing import Dict, List, Optional, Tuple

from network.vector import dominates, merge_vector_inplace
from store.kv_store import KeyValueStore, VersionedValue


class DataCenter:
    """Represents a geo-distributed replica with causal replication."""

    def __init__(self, dc_id: str, local_latency_ms: Tuple[int, int] = (3, 7)):
        self.dc_id = dc_id
        self.local_latency_ms = local_latency_ms
        self.kvstore = KeyValueStore()
        self._peers: Dict[str, Tuple["DataCenter", Tuple[int, int]]] = {}
        self.vector_clock: Dict[str, int] = {}
        self.buffered_updates: List[VersionedValue] = []
        self.metrics = {
            "causal_reads": [],
            "causal_writes": [],
            "linearizable_reads": [],
            "linearizable_writes": [],
        }
        self._lock = asyncio.Lock()

    def reset_metrics(self) -> None:
        for values in self.metrics.values():
            values.clear()

    def add_peer(self, peer: "DataCenter", latency_range_ms: Tuple[int, int]) -> None:
        self._peers[peer.dc_id] = (peer, latency_range_ms)

    def sample_local_latency(self) -> float:
        low, high = self.local_latency_ms
        return random.uniform(low, high) / 1000

    def latency_to(self, peer_id: str) -> Optional[float]:
        if peer_id == self.dc_id:
            return 0.0
        peer_entry = self._peers.get(peer_id)
        if not peer_entry:
            return None
        low, high = peer_entry[1]
        return random.uniform(low, high) / 1000

    async def client_put(
        self,
        key: str,
        value: str,
        session_vector: Dict[str, int],
        consistency: str,
    ) -> VersionedValue:
        start = time.perf_counter()
        async with self._lock:
            merge_vector_inplace(self.vector_clock, session_vector)
            self.vector_clock[self.dc_id] = self.vector_clock.get(self.dc_id, 0) + 1
            version = VersionedValue(
                key=key,
                value=value,
                origin=self.dc_id,
                version_vector=dict(self.vector_clock),
                dependencies=dict(session_vector),
                timestamp=time.time(),
            )
            self.kvstore.put(version)
            self._drain_buffer_locked()

        if consistency == "linearizable":
            await self._broadcast_update(version, wait_for_ack=True)
            bucket = "linearizable_writes"
        else:
            asyncio.create_task(self._broadcast_update(version, wait_for_ack=False))
            bucket = "causal_writes"

        await asyncio.sleep(self.sample_local_latency())
        elapsed = time.perf_counter() - start
        self.metrics[bucket].append(elapsed)
        return version

    async def client_get(
        self,
        key: str,
        session_vector: Dict[str, int],
        consistency: str,
    ) -> Optional[VersionedValue]:
        start = time.perf_counter()

        if consistency == "linearizable":
            value = await self._linearizable_read(key)
            bucket = "linearizable_reads"
        else:
            await self._wait_for_dependencies(session_vector)
            await asyncio.sleep(self.sample_local_latency())
            value = self.kvstore.latest(key)
            bucket = "causal_reads"

        elapsed = time.perf_counter() - start
        self.metrics[bucket].append(elapsed)
        if value:
            async with self._lock:
                merge_vector_inplace(self.vector_clock, value.version_vector)
                self._drain_buffer_locked()
        return value

    async def receive_update(self, version: VersionedValue) -> None:
        async with self._lock:
            if self._already_applied(version):
                return
            if not self._dependencies_satisfied(version):
                self.buffered_updates.append(version)
                return
            self._commit_version(version)
            self._drain_buffer_locked()

    async def _broadcast_update(self, version: VersionedValue, wait_for_ack: bool) -> None:
        if not self._peers:
            return

        async def send(peer_entry: Tuple["DataCenter", Tuple[int, int]]) -> None:
            peer, latency_range = peer_entry
            low, high = latency_range
            await asyncio.sleep(random.uniform(low, high) / 1000)
            replica_version = VersionedValue(
                key=version.key,
                value=version.value,
                origin=version.origin,
                version_vector=dict(version.version_vector),
                dependencies=dict(version.dependencies),
                timestamp=version.timestamp,
            )
            await peer.receive_update(replica_version)

        tasks = [asyncio.create_task(send(entry)) for entry in self._peers.values()]
        if wait_for_ack:
            await asyncio.gather(*tasks)

    def _already_applied(self, version: VersionedValue) -> bool:
        seen = self.vector_clock.get(version.origin, 0)
        target = version.version_vector.get(version.origin, 0)
        return seen >= target

    def _dependencies_satisfied(self, version: VersionedValue) -> bool:
        return dominates(self.vector_clock, version.dependencies)

    def _commit_version(self, version: VersionedValue) -> None:
        merge_vector_inplace(self.vector_clock, version.version_vector)
        self.kvstore.put(version)

    async def _wait_for_dependencies(self, session_vector: Dict[str, int]) -> None:
        while not dominates(self.vector_clock, session_vector):
            await asyncio.sleep(0.005)

    def _drain_buffer_locked(self) -> None:
        changed = True
        while changed:
            changed = False
            remaining: List[VersionedValue] = []
            for update in self.buffered_updates:
                if self._dependencies_satisfied(update) and not self._already_applied(update):
                    self._commit_version(update)
                    changed = True
                else:
                    remaining.append(update)
            self.buffered_updates = remaining

    async def _linearizable_read(self, key: str) -> Optional[VersionedValue]:
        async with self._lock:
            local_value = self.kvstore.latest(key)

        async def request_latest(peer_entry: Tuple["DataCenter", Tuple[int, int]]) -> Optional[VersionedValue]:
            peer, latency_range = peer_entry
            low, high = latency_range
            await asyncio.sleep(random.uniform(low, high) / 1000)
            async with peer._lock:
                return peer.kvstore.latest(key)

        tasks = [asyncio.create_task(request_latest(entry)) for entry in self._peers.values()]
        results = [local_value] + await asyncio.gather(*tasks) if tasks else [local_value]

        freshest: Optional[VersionedValue] = None
        best_time = -1.0
        for candidate in results:
            if candidate and candidate.timestamp > best_time:
                freshest = candidate
                best_time = candidate.timestamp
        return freshest
