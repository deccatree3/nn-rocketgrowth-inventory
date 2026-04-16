# 로켓그로스 입고 계획 자동화

쿠팡 로켓그로스 밀크런 입고 작업(FC별 재고 보충)을 자동화하는 Streamlit 대시보드.

## 기능

- **입고 계획 생성**: 쿠팡 재고현황 + WMS 재고현황 raw 파일 2개 업로드 → 자동 입고수량 산출
- **제품 마스터**: 박스낱수, 유통기한, WMS 바코드 매핑 DB 관리
- **재고 현황 & 경고**: 재고부족 임박, 유통기한 임박 하이라이트
- **이력 조회**: 과거 회차 입고 이력 및 추이 차트
- **엑셀 내보내기**: 쿠팡 업로드 양식 자동 생성

## 아키텍처

```
[Raw 업로드/쿠팡 API] → [Ingestion] → [Supabase Postgres] → [Planning Engine] → [Dashboard / Excel Export]
                                                                               └→ [Slack 알림 (중기)]
```

## 로컬 실행

```bash
cd app
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -e ".[dev]"

# 시크릿 설정
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# .streamlit/secrets.toml 의 DB URL 채우기

# DB 마이그레이션
alembic upgrade head

# 초기 마스터 이관 (1회성)
python scripts/import_master_from_template.py "../20260330(밀크런, 서현, 동탄1, 작업일 0326)/raw/로켓그로스-재고체크-0326 - 템플릿 수정중.xlsx"

# 앱 실행
streamlit run app.py
```

## 테스트

```bash
pytest
```

## 배포 (Streamlit Community Cloud)

1. Supabase 프로젝트 생성 → Database URL 복사
2. 이 레포를 GitHub에 push
3. https://share.streamlit.io → New app → 레포 선택 → Main file: `app/app.py`
4. App settings → Secrets → `secrets.toml.example` 내용 붙여넣고 값 채우기
