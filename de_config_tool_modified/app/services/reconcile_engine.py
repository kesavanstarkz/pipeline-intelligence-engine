"""
Reconcile engine — merges baseline + candidate with configurable strategy,
applies manual overrides, and produces a structured change log.
"""
import copy
from typing import Any
from app.models.schemas import ReconcileResult


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base (override wins on conflicts)."""
    result = copy.deepcopy(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def _set_by_path(obj: dict, dotted_path: str, value: Any) -> None:
    """Set a value on a nested dict using a dotted path string."""
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        obj = obj.setdefault(part, {})
    obj[parts[-1]] = value


def _get_by_path(obj: dict, dotted_path: str) -> Any:
    parts = dotted_path.split(".")
    for part in parts:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def reconcile(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    strategy: str = "candidate_wins",
    overrides: dict[str, Any] | None = None,
) -> ReconcileResult:
    change_log: list[dict[str, Any]] = []

    # Step 1: base merge by strategy
    if strategy == "candidate_wins":
        merged = _deep_merge(baseline, candidate)
    elif strategy == "baseline_wins":
        merged = _deep_merge(candidate, baseline)
    else:
        # manual — start from baseline, apply overrides only
        merged = copy.deepcopy(baseline)

    # Step 2: apply manual overrides (always win regardless of strategy)
    if overrides:
        for path, value in overrides.items():
            old_val = _get_by_path(merged, path)
            _set_by_path(merged, path, value)
            change_log.append({
                "path": path,
                "action": "override",
                "from": old_val,
                "to": value,
            })

    # Step 3: build change log for strategy-level changes
    from app.services.diff_engine import _flatten
    flat_base = _flatten(baseline)
    flat_cand = _flatten(candidate)
    flat_merged = _flatten(merged)

    for path in sorted(set(flat_base) | set(flat_cand)):
        bv = flat_base.get(path)
        cv = flat_cand.get(path)
        mv = flat_merged.get(path)

        # skip if override already logged this path
        if overrides and path in overrides:
            continue

        if bv != cv:
            change_log.append({
                "path": path,
                "action": "merged",
                "strategy": strategy,
                "baseline_val": bv,
                "candidate_val": cv,
                "result_val": mv,
            })

    # add metadata
    import datetime
    merged["_meta"] = {
        "reconcile_strategy": strategy,
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "overrides_applied": len(overrides) if overrides else 0,
    }

    return ReconcileResult(merged=merged, change_log=change_log)
