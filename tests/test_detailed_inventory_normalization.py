from engine.pipeline_engine import _normalize_detailed_inventory


def test_normalize_detailed_inventory_converts_strings_to_dicts():
    items = [
        "...",
        {"id": "aws || my-lambda", "service": "LAMBDA", "config": {"Runtime": "python3.10"}},
        "fabric || notebook-1",
        None,
    ]

    result = _normalize_detailed_inventory(items)

    assert result == [
        {"id": "...", "service": "UNKNOWN", "config": {}},
        {"id": "aws || my-lambda", "service": "LAMBDA", "config": {"Runtime": "python3.10"}},
        {"id": "fabric || notebook-1", "service": "UNKNOWN", "config": {}},
    ]
