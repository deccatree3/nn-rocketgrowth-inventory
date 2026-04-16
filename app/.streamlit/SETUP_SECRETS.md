# Secrets 설정 가이드

## 로컬 PC

1. `secrets.toml.example` 을 복사해서 `secrets.toml` 생성
2. 아래 내용을 붙여넣고 `YOUR_DB_PASSWORD` 부분만 직접 교체

```toml
[database]
# Supabase > Connect > Connection string > Transaction pooler URL
url = "postgresql+psycopg://postgres.zzesmdluvifmgjovwtuu:YOUR_DB_PASSWORD@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres"

[app]
low_stock_days_threshold = 14
near_expiry_ratio_threshold = 0.3
```

⚠️ 주의사항:
- `secrets.toml` 파일은 git에 절대 커밋되지 않음 (.gitignore에 포함됨)
- 비밀번호는 Claude(AI)에게 채팅으로 보내지 말 것 — 이 파일에 직접 편집
- Supabase 비밀번호를 잊었으면: Supabase Dashboard > Project Settings > Database > "Reset database password"

## 드라이버 방언

⚠️ 중요: URL 앞에 `postgresql+psycopg://` 를 반드시 붙일 것 (`postgresql://` 아님).
SQLAlchemy가 psycopg3 드라이버를 사용하도록 지정하는 부분입니다.

## Streamlit Community Cloud 배포 시

1. https://share.streamlit.io 로 이동
2. 앱 선택 → 우측 상단 ⋮ → **App settings** → **Secrets**
3. 위 `secrets.toml` 내용을 그대로 붙여넣기 → Save
4. 앱 자동 재시작

## 연결 테스트

```bash
cd app
python -c "from lib.db import get_engine; e=get_engine(); import sqlalchemy as sa; print(e.connect().execute(sa.text('select version()')).scalar())"
```

정상이면 PostgreSQL 버전이 출력됩니다.
