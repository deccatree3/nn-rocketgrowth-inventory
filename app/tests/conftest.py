"""pytest 공통 픽스처 — 실제 raw 파일 경로."""
from __future__ import annotations

from pathlib import Path

import pytest

# 레포 루트의 raw 데이터 폴더 (app/ 기준 한 단계 위)
RAW_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "20260330(밀크런, 서현, 동탄1, 작업일 0326)"
    / "raw"
)


@pytest.fixture(scope="session")
def coupang_raw_path() -> Path:
    p = RAW_DIR / "inventory_health_sku_info_20260326163821.xlsx"
    if not p.exists():
        pytest.skip(f"raw 파일 없음: {p}")
    return p


@pytest.fixture(scope="session")
def wms_raw_path() -> Path:
    p = RAW_DIR / "Document_2026-03-26.xls"
    if not p.exists():
        pytest.skip(f"raw 파일 없음: {p}")
    return p


@pytest.fixture(scope="session")
def template_path() -> Path:
    p = RAW_DIR / "로켓그로스-재고체크-0326 - 템플릿 수정중.xlsx"
    if not p.exists():
        pytest.skip(f"템플릿 없음: {p}")
    return p
