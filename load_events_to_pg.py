"""
Airflow DAG: load_web_events_to_postgres
DataLake(JSONL) → PostgreSQL 배치 적재 파이프라인

스케줄: 6시간마다 (00:00 / 06:00 / 12:00 / 18:00 UTC)
처리 대상: datalake/ 디렉토리의 events_YYYYMMDDHH.jsonl 파일 중
           현재 시간 파일을 제외한 완료된 파일 전부 (최대 6개 파일)
적재 완료: datalake/processed/ 폴더로 이동 → 중복 처리 방지
안전 장치: event_id UNIQUE + ON CONFLICT DO NOTHING → 멱등성 보장
"""

import os
import json
import glob
import shutil
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values, Json

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── 경로 & DB 설정 ────────────────────────────────────────────────────────────
DATALAKE_PATH  = os.environ.get("DATALAKE_PATH", "/datalake")
PROCESSED_PATH = os.path.join(DATALAKE_PATH, "processed")

DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME", "livedb"),
    "user":     os.environ.get("DB_USER", "liveuser"),
    "password": os.environ.get("DB_PASS", "livepass"),
    "host":     os.environ.get("DB_HOST", "postgres"),
    "port":     os.environ.get("DB_PORT", "5432"),
}

# ── Task 함수 ─────────────────────────────────────────────────────────────────

def create_table_if_not_exists():
    """
    web_events 테이블이 없으면 생성합니다.
    event_id에 UNIQUE 제약을 걸어 중복 적재를 DB 레벨에서 방어합니다.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS web_events (
                        id         SERIAL PRIMARY KEY,
                        event_id   UUID        UNIQUE NOT NULL,
                        timestamp  TIMESTAMP   NOT NULL,
                        user_id    VARCHAR(50),
                        event_type VARCHAR(50),
                        platform   VARCHAR(50),
                        details    JSONB
                    );
                """)
        print("web_events 테이블 확인 완료.")
    finally:
        conn.close()


def get_target_files():
    """
    datalake/ 에서 처리 대상 파일 목록을 반환합니다.
    Consumer가 현재 쓰는 중인 파일은 제외합니다.

    [운영용] 시간 단위 버킷 (주석 처리됨)
    # current_bucket = datetime.utcnow().strftime("%Y%m%d%H")

    [연습용] 5분 단위 버킷
    """
    # [운영용] 시간 단위: YYYYMMDDHH
    # now = datetime.utcnow()
    # current_bucket = now.strftime("%Y%m%d%H")

    # [연습용] 5분 단위 버킷: YYYYMMDDHHMM
    now = datetime.utcnow()
    minute_bucket = (now.minute // 5) * 5
    current_bucket = now.strftime("%Y%m%d%H") + f"{minute_bucket:02d}"

    all_files = sorted(glob.glob(os.path.join(DATALAKE_PATH, "events_*.jsonl")))

    target_files = [
        f for f in all_files
        if os.path.basename(f) != f"events_{current_bucket}.jsonl"
    ]

    print(f"전체 파일: {len(all_files)}개 | 처리 대상: {len(target_files)}개 (현재 버킷 {current_bucket} 제외)")
    return target_files


def load_events_to_postgres():
    """
    처리 대상 JSONL 파일을 읽어 PostgreSQL에 배치 INSERT합니다.
    완료된 파일은 processed/ 폴더로 이동하여 다음 실행에서 중복 처리를 방지합니다.
    """
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    target_files = get_target_files()

    if not target_files:
        print("처리할 파일이 없습니다. 종료합니다.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    total_inserted = 0
    total_skipped  = 0

    try:
        for filepath in target_files:
            filename = os.path.basename(filepath)
            print(f"\n[처리 시작] {filename}")

            # JSONL 파일 파싱
            batch = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        batch.append((
                            event["event_id"],
                            event["timestamp"],
                            event["user_id"],
                            event["event_type"],
                            event["platform"],
                            Json(event["details"]),  # JSONB
                        ))
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"  ⚠ 파싱 오류 (스킵): {e} | 내용: {line[:80]}")

            if not batch:
                print(f"  → 유효한 이벤트 없음. 파일 이동 후 다음으로.")
                _move_to_processed(filepath)
                continue

            # 배치 INSERT (event_id 중복 시 SKIP)
            with conn:
                with conn.cursor() as cur:
                    before_count = _get_row_count(cur)

                    execute_values(cur, """
                        INSERT INTO web_events
                            (event_id, timestamp, user_id, event_type, platform, details)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING
                    """, batch)

                    after_count = _get_row_count(cur)

            inserted = after_count - before_count
            skipped  = len(batch) - inserted
            total_inserted += inserted
            total_skipped  += skipped

            print(f"  ✅ 적재: {inserted}건 | 중복 스킵: {skipped}건 (파일 내 총 {len(batch)}건)")

            # 완료 파일 → processed/ 이동
            _move_to_processed(filepath)
            print(f"  📁 {filename} → processed/ 이동 완료")

    finally:
        conn.close()

    print(f"\n{'='*50}")
    print(f"배치 완료 | 총 적재: {total_inserted}건 | 총 스킵: {total_skipped}건")
    print(f"{'='*50}")


def _get_row_count(cur):
    cur.execute("SELECT COUNT(*) FROM web_events;")
    return cur.fetchone()[0]


def _move_to_processed(filepath):
    dest = os.path.join(PROCESSED_PATH, os.path.basename(filepath))
    shutil.move(filepath, dest)


# ── DAG 정의 ──────────────────────────────────────────────────────────────────

default_args = {
    "owner":        "airflow",
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
}

with DAG(
    dag_id="load_web_events_to_postgres",
    default_args=default_args,
    description="DataLake JSONL → PostgreSQL 배치 적재 (6시간마다)",
    # schedule_interval="0 */6 * * *",  # 00:00 / 06:00 / 12:00 / 18:00 (UTC)
    schedule_interval="*/5 * * * *",  # 테스트용: 5분마다 (운영 시: "0 */6 * * *")
    start_date=datetime(2026, 4, 21),
    catchup=False,
    tags=["web_events", "batch", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    t2 = PythonOperator(
        task_id="load_events_to_postgres",
        python_callable=load_events_to_postgres,
    )

    t1 >> t2
