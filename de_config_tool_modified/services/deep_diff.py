"""
deep_diff.py
~~~~~~~~~~~~
Recursive JSON diff engine for the DE Config Tool.

Returns a flat list of DiffNode dicts that the frontend can render as a tree.
Each node has:
  - path   : dot-separated key path  (e.g. "root.connection.host")
  - key    : the leaf key name
  - type   : "match" | "mismatch" | "added" | "removed" | "type_change" | "nested"
  - left   : value in *generated* config (None for removed)
  - right  : value in *azure* config     (None for added)
  - children: list of child DiffNodes (populated when both sides are dicts)
"""

from typing import Any


def deep_diff(left: Any, right: Any, path: str = "root") -> list[dict]:
    """
    Top-level entry point. Diffs two arbitrary JSON values.
    Always returns a *list* of DiffNode dicts.
    """
    # Both are dicts → recurse into keys
    if isinstance(left, dict) and isinstance(right, dict):
        return _diff_dicts(left, right, path)

    # Both are lists → element-wise diff
    if isinstance(left, list) and isinstance(right, list):
        return _diff_lists(left, right, path)

    # Scalar comparison
    return [_scalar_node(path, left, right)]


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _diff_dicts(left: dict, right: dict, path: str) -> list[dict]:
    nodes: list[dict] = []
    all_keys = sorted(set(left) | set(right))

    for key in all_keys:
        child_path = f"{path}.{key}"
        in_left = key in left
        in_right = key in right

        if in_left and not in_right:
            nodes.append(_make_node(child_path, key, "removed", left[key], None))

        elif in_right and not in_left:
            nodes.append(_make_node(child_path, key, "added", None, right[key]))

        else:
            lv, rv = left[key], right[key]

            # Both sides are dicts → nested diff
            if isinstance(lv, dict) and isinstance(rv, dict):
                children = _diff_dicts(lv, rv, child_path)
                nodes.append({
                    "path": child_path,
                    "key": key,
                    "type": "nested",
                    "left": None,
                    "right": None,
                    "children": children,
                })

            # Both sides are lists → list diff
            elif isinstance(lv, list) and isinstance(rv, list):
                children = _diff_lists(lv, rv, child_path)
                nodes.append({
                    "path": child_path,
                    "key": key,
                    "type": "nested",
                    "left": None,
                    "right": None,
                    "children": children,
                })

            # Type mismatch (one dict, one scalar, etc.)
            elif type(lv) != type(rv) and not _both_numeric(lv, rv):
                nodes.append(_make_node(child_path, key, "type_change", lv, rv))

            # Scalar comparison
            else:
                nodes.append(_scalar_node(child_path, lv, rv, key=key))

    return nodes


def _diff_lists(left: list, right: list, path: str) -> list[dict]:
    nodes: list[dict] = []
    max_len = max(len(left), len(right))

    for i in range(max_len):
        child_path = f"{path}[{i}]"
        if i >= len(left):
            nodes.append(_make_node(child_path, f"[{i}]", "added", None, right[i]))
        elif i >= len(right):
            nodes.append(_make_node(child_path, f"[{i}]", "removed", left[i], None))
        else:
            lv, rv = left[i], right[i]
            if isinstance(lv, dict) and isinstance(rv, dict):
                children = _diff_dicts(lv, rv, child_path)
                nodes.append({
                    "path": child_path,
                    "key": f"[{i}]",
                    "type": "nested",
                    "left": None,
                    "right": None,
                    "children": children,
                })
            else:
                nodes.append(_scalar_node(child_path, lv, rv, key=f"[{i}]"))

    return nodes


def _scalar_node(path: str, lv: Any, rv: Any, key: str = "") -> dict:
    if not key:
        key = path.rsplit(".", 1)[-1]
    # Normalise for comparison: strip whitespace on strings, case-insensitive
    lv_cmp = lv.strip().lower() if isinstance(lv, str) else lv
    rv_cmp = rv.strip().lower() if isinstance(rv, str) else rv
    node_type = "match" if lv_cmp == rv_cmp else "mismatch"
    return _make_node(path, key, node_type, lv, rv)


def _make_node(path: str, key: str, node_type: str, left: Any, right: Any) -> dict:
    return {
        "path": path,
        "key": key,
        "type": node_type,
        "left": left,
        "right": right,
        "children": [],
    }


def _both_numeric(a: Any, b: Any) -> bool:
    return isinstance(a, (int, float)) and isinstance(b, (int, float))
