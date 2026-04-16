"""쿠팡 결과 PDF 파서 회귀 테스트."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from lib.coupang_result import (
    name_similarity,
    normalize_product_name,
    parse_attachment_doc,
    parse_barcode_labels,
)

SAMPLE_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "20260330(밀크런, 서현, 동탄1, 작업일 0326)"
    / "sample"
    / "3. 입력 - 쿠팡 결과물"
)


@pytest.fixture(scope="session")
def label_pdf_path() -> Path:
    p = SAMPLE_DIR / "sku-barcode-labels-20260411_181300.pdf"
    if not p.exists():
        pytest.skip(f"sample 없음: {p}")
    return p


@pytest.fixture(scope="session")
def attachment_pdf_path() -> Path:
    p = SAMPLE_DIR / "물류부착문서_20260411_181331.pdf"
    if not p.exists():
        pytest.skip(f"sample 없음: {p}")
    return p


def test_label_pdf_parses_known_skus(label_pdf_path):
    labels = parse_barcode_labels(label_pdf_path)
    # 9개 SKU 가 라벨 PDF 에 있음
    assert len(labels) == 9
    # 총 라벨 카운트 = 358
    assert sum(l.count for l in labels.values()) == 358

    # 특정 SKU 검증
    info = labels.get("S0035577179371")  # 데일리키토 4개입
    assert info is not None
    assert info.count == 156
    assert info.expiry == date(2028, 3, 18)

    info2 = labels.get("8809744301563")  # 88코드 단품 (낙산균 번들/2개입)
    assert info2 is not None
    assert info2.count == 32
    assert info2.expiry == date(2028, 1, 21)


def test_attachment_pdf_meta(attachment_pdf_path):
    meta = parse_attachment_doc(attachment_pdf_path)
    assert meta.milkrun_id == "9946685"
    assert meta.fc_name == "동탄1"
    assert meta.fc_code == "17"
    assert meta.arrival_date == date(2026, 4, 14)
    assert meta.company_name == "주식회사 서현커머스"
    assert meta.box_barcode == "MRN9946685"
    # 총 팔레트수 = 4 (X-N 의 X 값, 페이지 수가 아님)
    assert meta.total_pallets == 4
    # 4개 고유 라벨
    labels = [p["label"] for p in meta.pallets]
    assert sorted(labels) == ["4-1", "4-2", "4-3", "4-4"]


def test_normalize_product_name():
    assert normalize_product_name("닥터키토 방탄커피(10포)") == normalize_product_name(
        "닥터키토방탄커피10p"
    )
    assert normalize_product_name(None) == ""


def test_name_similarity_high_for_similar():
    a = "퍼펙토 낙산균 포스트바이오틱스 프롤린 루테리 가세리 30p, 2개 60g"
    b = "퍼펙토 낙산균 포스트바이오틱스 프롤린 루테리 가세리, 30포, 2개입"
    sim = name_similarity(a, b)
    assert sim >= 0.7  # 높은 유사도


def test_name_similarity_low_for_different():
    sim = name_similarity("닥터키토 방탄커피", "스키니퓨리티 슈링티")
    assert sim < 0.3
