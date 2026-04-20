"""Tests for local workspace discovery."""
from __future__ import annotations

from pathlib import Path

import pytest

from config.settings import settings
from engine.discovery.local_scanner import resolve_safe_root, scan_local_workspace


def test_resolve_safe_root_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    sub = tmp_path / "proj"
    sub.mkdir()
    p = resolve_safe_root(str(sub), settings)
    assert p.resolve() == sub.resolve()


def test_resolve_safe_root_rejects_outside(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    outside = tmp_path.parent / "other"
    outside.mkdir(exist_ok=True)
    with pytest.raises(ValueError, match="must be under"):
        resolve_safe_root(str(outside), settings)


def test_scan_finds_dbt_marker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dbt_project.yml").write_text("name: test\nversion: 1\n", encoding="utf-8")
    disc = scan_local_workspace(tmp_path, max_depth=3, max_files_recorded=50)
    assert any("dbt" in f.lower() for f in disc["frameworks"])
    assert disc["generated_ingestion_config"]["version"] == 1
