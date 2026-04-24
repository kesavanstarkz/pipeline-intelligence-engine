"""
Deep diff engine — fully recursive, path-aware.
Handles nested dicts, lists, and scalar values at any depth.
"""
from typing import Any
from app.models.schemas import DiffEntry, DiffResult


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten a nested dict/list into dotted-path keys."""
    result: dict[str, Any] = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                result.update(_flatten(v, full_key))
            else:
                result[full_key] = v

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            full_key = f"{prefix}[{i}]"
            if isinstance(v, (dict, list)):
                result.update(_flatten(v, full_key))
            else:
                result[full_key] = v
    else:
        result[prefix] = obj

    return result


def deep_diff(baseline: dict[str, Any], candidate: dict[str, Any]) -> DiffResult:
    flat_base = _flatten(baseline)
    flat_cand = _flatten(candidate)

    all_keys = set(flat_base) | set(flat_cand)
    entries: list[DiffEntry] = []

    counts = {"match": 0, "mismatch": 0, "added": 0, "removed": 0}

    for path in sorted(all_keys):
        in_base = path in flat_base
        in_cand = path in flat_cand

        if in_base and in_cand:
            bv, cv = flat_base[path], flat_cand[path]
            if bv == cv:
                kind = "match"
            else:
                kind = "mismatch"
            entries.append(DiffEntry(path=path, type=kind, baseline_val=bv, candidate_val=cv))
        elif in_cand and not in_base:
            entries.append(DiffEntry(path=path, type="added", candidate_val=flat_cand[path]))
        else:
            entries.append(DiffEntry(path=path, type="removed", baseline_val=flat_base[path]))

        counts[entries[-1].type] += 1

    return DiffResult(summary=counts, entries=entries)
