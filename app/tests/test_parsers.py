"""파서 회귀 테스트 — 실제 raw 파일을 파싱한 결과를 고정값으로 검증."""
from __future__ import annotations

from datetime import date

from lib.ingestion.coupang_file import parse_coupang_inventory_file
from lib.ingestion.wms_file import aggregate_wms_by_barcode, parse_wms_inventory_file


def test_coupang_parse(coupang_raw_path):
    snap = parse_coupang_inventory_file(coupang_raw_path)
    assert snap.snapshot_date == date(2026, 3, 26)
    assert snap.source_type == "file"
    assert len(snap.rows) == 137

    # 1번 행 = 에브리 포스트바이오틱스 롤필름 스페셜본드 스틱
    first = snap.rows[0]
    assert first.coupang_option_id == 93264049371
    assert first.coupang_product_id == 11641866182
    assert first.sku_id == 13927374
    assert first.orderable_stock == 1034
    assert first.inbound_stock == 0
    assert first.sales_qty_7d == 3
    assert first.sales_qty_30d == 13

    # 전체 합계 스냅샷 (회귀 방지)
    assert sum(r.orderable_stock for r in snap.rows) == 8080
    assert sum(r.sales_qty_7d for r in snap.rows) == 1376
    assert sum(r.sales_qty_30d for r in snap.rows) == 6469


def test_wms_parse(wms_raw_path):
    snap = parse_wms_inventory_file(wms_raw_path)
    assert snap.snapshot_date == date(2026, 3, 26)
    assert len(snap.rows) >= 120

    agg = aggregate_wms_by_barcode(snap)
    assert len(agg) >= 30

    bc = "8809647580041"
    assert bc in agg
    assert agg[bc]["total_qty"] >= 500
    # batches 키 존재
    assert "batches" in agg[bc]


def test_wms_aggregation_sums_match(wms_raw_path):
    """RELEASEAREA 제외한 raw 합계가 aggregation 과 일치해야 한다."""
    snap = parse_wms_inventory_file(wms_raw_path)
    agg = aggregate_wms_by_barcode(snap)
    total_sum_rows = sum(
        (r.total_qty or 0)
        for r in snap.rows
        if (r.loc or "").upper() != "RELEASEAREA"
    )
    total_sum_agg = sum(v["total_qty"] for v in agg.values())
    assert total_sum_rows == total_sum_agg
    # 배치별 합이 바코드별 총합과 일치
    for bc, v in agg.items():
        batch_total = sum(b["total"] for b in v["batches"])
        assert batch_total == v["total_qty"], f"{bc}: batch sum mismatch"


def test_wms_releasearea_excluded(wms_raw_path):
    """LOC=RELEASEAREA 행은 집계에서 제외되어야 한다."""
    snap = parse_wms_inventory_file(wms_raw_path)
    agg = aggregate_wms_by_barcode(snap)

    # raw 에는 RELEASEAREA 행이 존재
    release_rows = [r for r in snap.rows if (r.loc or "").upper() == "RELEASEAREA"]
    assert len(release_rows) > 0, "테스트 파일에 RELEASEAREA 행이 있어야 함"

    # 집계 결과에서 제외 확인: 8809647580041 은 RELEASEAREA에 42개 있었음
    #   → 집계 total_qty 는 raw total 에서 42를 뺀 값
    bc = "8809647580041"
    raw_total_all = sum(r.total_qty or 0 for r in snap.rows if r.barcode == bc)
    raw_total_release = sum(
        r.total_qty or 0 for r in snap.rows if r.barcode == bc and (r.loc or "").upper() == "RELEASEAREA"
    )
    assert raw_total_release > 0, "8809647580041 의 RELEASEAREA 수량이 있어야 함"
    assert agg[bc]["total_qty"] == raw_total_all - raw_total_release


def test_wms_multi_batch_detected(wms_raw_path):
    """실제 raw 파일에서 복수 배치 바코드가 존재해야 한다."""
    snap = parse_wms_inventory_file(wms_raw_path)
    agg = aggregate_wms_by_barcode(snap)
    multi = {bc: v for bc, v in agg.items() if len([b for b in v["batches"] if b["expiry"]]) > 1}
    assert len(multi) >= 5, f"복수 배치 바코드가 기대치 미만: {len(multi)}"
    # 샘플 검증: 8809647580041은 두 배치여야 함
    bc = "8809647580041"
    assert bc in multi
    dated_batches = [b for b in agg[bc]["batches"] if b["expiry"]]
    assert len(dated_batches) == 2
    # 유통일 오름차순
    assert dated_batches[0]["expiry"] < dated_batches[1]["expiry"]
