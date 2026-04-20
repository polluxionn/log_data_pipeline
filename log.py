import os
import uuid
import random
import time
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta

# PostgreSQL 데이터베이스 설정
# 도커 컨테이너 환경을 위해 환경 변수로 주입받도록 처리. 
# 환경 변수가 없으면 localhost의 기본 설정이 사용됩니다.
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME", "log_db"),
    "user": os.environ.get("DB_USER", "liveuser"),
    "password": os.environ.get("DB_PASS", "livepass"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432")
}

def init_db():
    # 도커 Compose로 함께 띄워질 때 DB가 준비될 때까지 시간을 벌기 위해 연결 재시도 로직 추가
    max_retries = 10
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS web_events (
                    id SERIAL PRIMARY KEY,
                    event_id UUID,
                    timestamp TIMESTAMP,
                    user_id VARCHAR(50),
                    event_type VARCHAR(50),
                    platform VARCHAR(50),
                    details JSONB
                )
            """)
            conn.commit()
            return conn, cur
        except psycopg2.OperationalError as e:
            print(f"DB 연결 대기 중... ({attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    
    raise Exception("데이터베이스에 연결할 수 없습니다. 설정을 확인해주세요.")

def insert_event(cur, event):
    cur.execute("""
        INSERT INTO web_events (event_id, timestamp, user_id, event_type, platform, details)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        event["event_id"],
        event["timestamp"],
        event["user_id"],
        event["event_type"],
        event["platform"],
        Json(event["details"])
    ))

def generate_events(is_historical=False):
    PLATFORM = ["IOS", "ANDROID", "WINDOW", "MAC"]
    ENDPOINTS = ["/home", "/products", "/cart", "/order", "/login"]
    
    # 10% 확률로 에러 발생, 그 외 90%는 정상 접근
    is_error = random.random() < 0.1 
    
    platform = random.choice(PLATFORM)
    endpoint = random.choice(ENDPOINTS)
    
    user_id = f"user_{random.randint(1, 100)}"
    
    if is_historical:
        # 초기 1000개 데이터는 최근 2일(2 * 24 * 60 * 60 초) 내의 무작위 과거 시간으로 설정합니다.
        random_seconds_ago = random.randint(0, 2 * 24 * 60 * 60)
        base_time = datetime.utcnow() - timedelta(seconds=random_seconds_ago)
    else:
        # 실시간 데이터는 현재 시간(`utcnow()`)을 사용합니다.
        base_time = datetime.utcnow()
    
    events_to_return = []
    
    # 어떤 행동이든 사용자가 해당 엔드포인트를 보았다는 PAGE_VIEW가 우선 생성됩니다!
    base_pageview = {
        "event_id": str(uuid.uuid4()),
        "timestamp": base_time.isoformat(),
        "user_id": user_id,
        "platform": platform,
        "event_type": "PAGE_VIEW",
        "details": {
            "endpoint": endpoint,
            "status_code": 200
        }
    }
    events_to_return.append(base_pageview)
    
    # 뒤따라오는 두 번째 이벤트(로그인/구매/에러)는 보통 페이지뷰 직후 발생하므로 0.05초~0.3초 미세 딜레이를 더해 리얼리티를 살립니다.
    action_time = (base_time + timedelta(milliseconds=random.randint(50, 300))).isoformat()
    
    if is_error:
        # 에러 발생 시 (페이지뷰 이후 에러 로그가 찍힘)
        error_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": action_time,
            "user_id": user_id,
            "platform": platform,
            "event_type": "ERROR",
            "details": {
                "endpoint": endpoint,
                "status_code": random.choice([403, 404, 500]),
                "msg": "Connection Timeout" if random.random() < 0.5 else "Internal Server Error"
            }
        }
        events_to_return.append(error_event)
    else:
        # 정상 페이지 접근 시 추가 액션(이벤트) 로그가 생성됩니다.
        if endpoint == "/login":
            login_event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": action_time,
                "user_id": user_id,
                "platform": platform,
                "event_type": "LOGIN",
                "details": {
                    "endpoint": endpoint,
                    "status_code": 200,
                    "method": "POST"
                }
            }
            events_to_return.append(login_event)
        elif endpoint == "/order":
            purchase_event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": action_time,
                "user_id": user_id,
                "platform": platform,
                "event_type": "PURCHASE",
                "details": {
                    "endpoint": endpoint,
                    "amount": random.randrange(1000, 100000, 100),
                    "item_id": f"item_{random.randint(1, 50)}",
                    "status_code": 200
                }
            }
            events_to_return.append(purchase_event)
            
    return events_to_return

if __name__ == "__main__":
    print("PostgreSQL 데이터베이스 연결을 시도합니다...")
    try:
        conn, cur = init_db()
        print("데이터베이스 연결 성공 및 테이블 초기화 완료!")
    except Exception as e:
        print(f"DB 연결 실패: {e}")
        exit(1)
        
    print("초기 데이터 약 1000건을 생성하여 DB에 저장합니다... (최근 2일간 쌓인 과거 데이터)")
    inserted_count = 0
    target_count = 1000
    while inserted_count < target_count:
        events = generate_events(is_historical=True)
        for ev in events:
            insert_event(cur, ev)
            inserted_count += 1
            if inserted_count >= target_count:
                break
    conn.commit()
    print(f"과거 초기 {inserted_count}건 데이터베이스 저장 완료!")
    
    print("이제부터 실시간 로그 생성을 시작합니다 (초당 약 2~3세트) - 종료하려면 Ctrl+C를 누르세요.")
    try:
        while True:
            # 초당 2~3회의 사용량 시뮬레이션을 위해 대기
            time.sleep(random.uniform(0.33, 0.5))
            
            # 여기서 발생된 events 안에는 PAGE_VIEW와 부가 로그인/결제/에러 이벤트가 쌍으로 들어있을 수 있습니다.
            events = generate_events(is_historical=False)
            for ev in events:
                insert_event(cur, ev)
                print(f"[{ev['timestamp']}] 로그 발생: {ev['event_type']:>9} | User: {ev['user_id']:>8} | Endpoint: {ev['details']['endpoint']}")
            conn.commit()
            print("-" * 60) # 이벤트를 쌍으로 묶어서 보기 편하게 구분선 추가
            
    except KeyboardInterrupt:
        print("\n사용자에 의해 로그 생성을 종료합니다.")
    finally:
        cur.close()
        conn.close()
        print("데이터베이스 연결이 안전하게 해제되었습니다.")