import asyncio
import time
from typing import Dict, Optional

from network.vector import merge_vector_inplace


class ClientSession:
    """Client that tracks its causal context via a session vector."""

    def __init__(self, name: str, home_dc, leader_dc=None):
        self.name = name
        self.home_dc = home_dc
        self.leader_dc = leader_dc or home_dc
        self.session_vector: Dict[str, int] = {}
        self.metrics = {
            "causal_reads": [],
            "causal_writes": [],
            "linearizable_reads": [],
            "linearizable_writes": [],
        }

    def reset_metrics(self) -> None:
        for values in self.metrics.values():
            values.clear()

    async def put(self, key: str, value: str, consistency: str = "causal") -> None:
        start = time.perf_counter()
        target_dc = self._target_dc(consistency)
        await self._simulate_client_latency(target_dc)
        version = await target_dc.client_put(key, value, dict(self.session_vector), consistency)
        merge_vector_inplace(self.session_vector, version.version_vector)
        elapsed = time.perf_counter() - start
        self.metrics[f"{consistency}_writes"].append(elapsed)

    async def get(self, key: str, consistency: str = "causal") -> Optional[str]:
        start = time.perf_counter()
        target_dc = self._target_dc(consistency)
        await self._simulate_client_latency(target_dc)
        version = await target_dc.client_get(key, dict(self.session_vector), consistency)
        elapsed = time.perf_counter() - start
        self.metrics[f"{consistency}_reads"].append(elapsed)
        if version:
            merge_vector_inplace(self.session_vector, version.version_vector)
            return version.value
        return None

    def _target_dc(self, consistency: str):
        if consistency == "linearizable":
            return self.leader_dc
        return self.home_dc

    async def _simulate_client_latency(self, target_dc) -> None:
        if target_dc.dc_id == self.home_dc.dc_id:
            await asyncio.sleep(self.home_dc.sample_local_latency())
            return
        latency = self.home_dc.latency_to(target_dc.dc_id)
        if latency is None:
            latency = 0.08  # fallback for unknown routes
        await asyncio.sleep(latency)
