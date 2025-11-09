from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class VersionedValue:
    key: str
    value: str
    origin: str
    version_vector: Dict[str, int]
    dependencies: Dict[str, int]
    timestamp: float


class KeyValueStore:
    """In-memory multi-version store keyed by logical timestamps."""

    def __init__(self):
        self._data: Dict[str, List[VersionedValue]] = {}

    def put(self, versioned_value: VersionedValue) -> None:
        versions = self._data.setdefault(versioned_value.key, [])
        versions.append(versioned_value)
        versions.sort(key=lambda vv: sum(vv.version_vector.values()))

    def latest(self, key: str) -> Optional[VersionedValue]:
        versions = self._data.get(key)
        if not versions:
            return None
        return versions[-1]

    def all_versions(self, key: str) -> List[VersionedValue]:
        return list(self._data.get(key, []))

    def keys(self) -> List[str]:
        return list(self._data.keys())
