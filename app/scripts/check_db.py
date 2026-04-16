"""DB 설정 및 연결 진단 스크립트 (비밀번호 절대 노출 안 함)."""
from __future__ import annotations

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def mask(v: str) -> str:
    s = str(v)
    if len(s) <= 2:
        return "*" * len(s)
    return s[0] + "*" * (len(s) - 2) + s[-1]


def diagnose_secrets() -> None:
    p = ROOT / ".streamlit" / "secrets.toml"
    if not p.exists():
        print(f"[!] {p} 없음")
        return
    with p.open("rb") as f:
        data = tomllib.load(f)
    db = data.get("database", {})
    print(f"[database] keys: {sorted(db.keys())}")
    for k in sorted(db.keys()):
        v = db[k]
        if k in ("password", "url"):
            print(f"  {k}: (length={len(str(v))}, 마스킹됨)")
        else:
            print(f"  {k}: {v}")

    # 권장 형식 경고
    if "url" in db:
        print()
        print("[!] 경고: [database].url 이 설정되어 있습니다.")
        print("    비밀번호에 특수문자(#, @, :, /)가 있으면 파싱 오류가 발생할 수 있습니다.")
        print("    권장: host/port/user/password/dbname 5개 필드로 분리")
    elif all(k in db for k in ("host", "user", "password")):
        print("[OK] 분리 필드 형식 사용 중")


def test_connect() -> None:
    try:
        from lib.db import get_engine
        import sqlalchemy as sa

        eng = get_engine()
        with eng.connect() as conn:
            ver = conn.execute(sa.text("select version()")).scalar()
        print(f"[OK] 연결 성공: {str(ver)[:60]}...")
    except Exception as e:
        # 예외 메시지에도 비밀번호가 들어있을 수 있으므로 마스킹
        msg = str(e)
        import re
        # 1) postgres://user:pass@host 형태 → ***
        msg = re.sub(r"(://[^:@]+:)[^@]+(@)", r"\1***\2", msg)
        # 2) 'failed to resolve host \'...@host\'' 처럼 password 파편이 포함될 수 있으므로 @ 이전 부분은 잘라냄
        msg = re.sub(r"host '[^']*@([^']+)'", r"host '\1'", msg)
        print(f"[FAIL] {type(e).__name__}: {msg[:400]}")


if __name__ == "__main__":
    print("=== Secrets 진단 ===")
    diagnose_secrets()
    print()
    print("=== DB 연결 테스트 ===")
    test_connect()
