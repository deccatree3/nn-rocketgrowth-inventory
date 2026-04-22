"""Streamlit/CLI 공통 설정 로더.

Streamlit 실행 시에는 `st.secrets`을, CLI/테스트 실행 시에는 환경변수를 사용한다.

DB 접속 정보는 두 가지 형태 모두 지원:
  (1) [database] url = "postgresql+psycopg://user:pass@host:port/db"
  (2) [database] host = "..", port = 5432, user = "..", password = "..", dbname = ".."
      → (2)는 비밀번호에 특수문자가 있어도 URL 인코딩 불필요
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    # 대시보드 경고 임계값
    low_stock_days_threshold: int = 14
    near_expiry_ratio_threshold: float = 0.3
    # 입고 계산 엔진 파라미터
    lead_time_days: int = 7           # 밀크런 주문 → FC 입고 완료까지 일수 (출고~판매시작)
    target_cover_days: int = 14       # 도착 시점 확보할 판매 가능 일수 (주1회 운영 기준)
    velocity_alpha: float = 0.4       # 7일 평균 가중치 (0~1), 나머지는 30일 평균 비중
    overstock_days: int = 35          # 이 일수 초과 시 과잉재고 경고
    reproduction_lead_days: int = 28  # 제품 재생산 리드타임 (재발주 알림 기준)
    # 멀티 업체 (MVP: 단일 업체)
    default_company_name: str = "서현"
    default_shipment_type: str = "milkrun"  # milkrun | parcel
    pallet_size_boxes: int = 19             # 1팔레트당 박스수 (안전 여유 — 20으로 하면 높이 초과 발생)


def _build_url_from_parts(db: dict) -> str | None:
    host = db.get("host")
    if not host:
        return None
    user = db.get("user", "postgres")
    password = db.get("password", "")
    port = int(db.get("port", 5432))
    dbname = db.get("dbname", "postgres")
    driver = db.get("driver", "postgresql+psycopg")
    return f"{driver}://{quote_plus(str(user))}:{quote_plus(str(password))}@{host}:{port}/{dbname}"


def _resolve_database_url(db_section: dict) -> str:
    url = db_section.get("url")
    if url:
        return url
    built = _build_url_from_parts(db_section)
    if built:
        return built
    raise RuntimeError("[database] 섹션에 url 또는 (host/user/password/...) 가 필요합니다.")


def load_config() -> AppConfig:
    # 1) Streamlit 런타임
    try:
        import streamlit as st  # type: ignore

        if hasattr(st, "secrets") and "database" in st.secrets:
            db_section = dict(st.secrets["database"])
            app_section = dict(st.secrets.get("app", {}))
            planning_section = dict(st.secrets.get("planning", {}))
            return AppConfig(
                database_url=_resolve_database_url(db_section),
                low_stock_days_threshold=int(app_section.get("low_stock_days_threshold", 14)),
                near_expiry_ratio_threshold=float(
                    app_section.get("near_expiry_ratio_threshold", 0.3)
                ),
                lead_time_days=int(planning_section.get("lead_time_days", 7)),
                target_cover_days=int(planning_section.get("target_cover_days", 14)),
                velocity_alpha=float(planning_section.get("velocity_alpha", 0.4)),
                overstock_days=int(planning_section.get("overstock_days", 35)),
                reproduction_lead_days=int(planning_section.get("reproduction_lead_days", 28)),
                default_company_name=str(planning_section.get("default_company_name", "서현")),
                default_shipment_type=str(planning_section.get("default_shipment_type", "milkrun")),
                pallet_size_boxes=int(planning_section.get("pallet_size_boxes", 19)),
            )
    except Exception:
        pass

    # 2) 로컬 CLI/테스트: secrets.toml 직접 로드
    secrets_path = Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml"
    if secrets_path.exists():
        with secrets_path.open("rb") as f:
            data = tomllib.load(f)
        db_section = data.get("database", {})
        app_section = data.get("app", {})
        planning_section = data.get("planning", {})
        if db_section:
            return AppConfig(
                database_url=_resolve_database_url(db_section),
                low_stock_days_threshold=int(app_section.get("low_stock_days_threshold", 14)),
                near_expiry_ratio_threshold=float(
                    app_section.get("near_expiry_ratio_threshold", 0.3)
                ),
                lead_time_days=int(planning_section.get("lead_time_days", 7)),
                target_cover_days=int(planning_section.get("target_cover_days", 14)),
                velocity_alpha=float(planning_section.get("velocity_alpha", 0.4)),
                overstock_days=int(planning_section.get("overstock_days", 35)),
                reproduction_lead_days=int(planning_section.get("reproduction_lead_days", 28)),
                default_company_name=str(planning_section.get("default_company_name", "서현")),
                default_shipment_type=str(planning_section.get("default_shipment_type", "milkrun")),
                pallet_size_boxes=int(planning_section.get("pallet_size_boxes", 19)),
            )

    # 3) 환경변수
    url = os.getenv("DATABASE_URL")
    if url:
        return AppConfig(
            database_url=url,
            low_stock_days_threshold=int(os.getenv("LOW_STOCK_DAYS_THRESHOLD", "14")),
            near_expiry_ratio_threshold=float(os.getenv("NEAR_EXPIRY_RATIO_THRESHOLD", "0.3")),
        )

    raise RuntimeError(
        "DB 설정이 없습니다. .streamlit/secrets.toml 또는 DATABASE_URL 환경변수를 설정하세요."
    )
