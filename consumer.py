import os
import json
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

# 환경변수 셋업
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME", "livedb"),
    "user": os.environ.get("DB_USER", "liveuser"),
    "password": os.environ.get("DB_PASS", "livepass"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432")
}

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "web_events")
DATALAKE_PATH = os.environ.get("DATALAKE_PATH", "./datalake")

def init_db():
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
    raise Exception("데이터베이스에 연결할 수 없습니다.")

def insert_to_db(cur, event):
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

def get_kafka_consumer():
    max_retries = 15
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='earliest',  # 구독 전 못 받은 데이터부터 전부 받기
                enable_auto_commit=True,
                group_id='web-events-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except Exception as e:
            print(f"Kafka 컨슈머 연결 대기 중... ({attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise Exception("Kafka 컨슈머에 연결할 수 없습니다.")

def append_to_datalake(event):
    # 날짜별로 파일명을 분류합니다. (timestamp 예시: 2026-04-20T13:45... 앞 10자리 추출)
    date_str = event["timestamp"][:10].replace("-", "") 
    
    # 폴더가 없으면 만듭니다.
    if not os.path.exists(DATALAKE_PATH):
        os.makedirs(DATALAKE_PATH)
        
    filepath = os.path.join(DATALAKE_PATH, f"events_{date_str}.jsonl")
    
    # JSONL(JSON Lines) 구조로 파일의 끝에 데이터를 한 줄씩 추가(Append)합니다.
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, ensure_ascii=False) + '\n')

if __name__ == "__main__":
    print("PostgreSQL 데이터베이스 연결을 시도합니다...")
    conn, cur = init_db()
    print("데이터베이스 연결 성공 및 테이블 초기화 완료!")

    print("Kafka 컨슈머를 초기화합니다...")
    # 브로커가 떠도 바로 연결이 되지 않을 수 있으니 시간 여유를 줍니다.
    consumer = get_kafka_consumer()

    print(f"[{KAFKA_TOPIC}] 토픽 구독 시작. 이벤트를 가져오는 중입니다...")
    
    try:
        for message in consumer:
            event = message.value
            
            # 카프카에서 컨슘한 이벤트를 DB에 동기화
            insert_to_db(cur, event)
            conn.commit()
            
            # 카프카에서 컨슘한 이벤트를 무조건 Data Lake에 JSON Lines로 적재
            append_to_datalake(event)
            
            # 로그 출력 길이를 줄이기 위해 간단히만 표시
            print(f"[Consumer] 적재 완료: {event['timestamp']} | {event['event_type']:>9} | DataLake & DB")
            
    except KeyboardInterrupt:
        print("\n컨슈머를 종료합니다.")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        print("모든 연결이 안전하게 종료되었습니다.")
