from typing import Dict


def dominates(current: Dict[str, int], target: Dict[str, int]) -> bool:
    """Return True if the current vector is >= target for all entries."""
    for dc_id, counter in target.items():
        if current.get(dc_id, 0) < counter:
            return False
    return True


def merge_vector(base: Dict[str, int], incoming: Dict[str, int]) -> Dict[str, int]:
    merged = dict(base)
    for dc_id, counter in incoming.items():
        if counter > merged.get(dc_id, 0):
            merged[dc_id] = counter
    return merged


def merge_vector_inplace(base: Dict[str, int], incoming: Dict[str, int]) -> Dict[str, int]:
    for dc_id, counter in incoming.items():
        if counter > base.get(dc_id, 0):
            base[dc_id] = counter
    return base
