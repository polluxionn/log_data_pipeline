import os
import json
import time
import datetime
from kafka import KafkaConsumer

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "web_events")
DATALAKE_PATH = os.environ.get("DATALAKE_PATH", "./datalake")

def get_kafka_consumer():
    """Kafka 브로커가 완전히 준비될 때까지 재시도하며 컨슈머를 초기화합니다."""
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
    """
    이벤트를 5분 단위 버킷 JSONL 파일에 추가합니다.

    [운영용] 시간 단위 (주석 처리됨)
    예: events_2026042115.jsonl → 2026년 4월 21일 15시(UTC) 이벤트
    # hour_str = datetime.datetime.utcnow().strftime("%Y%m%d%H")

    [연습용] 5분 단위 버킷
    예: events_202604211315.jsonl → 13:15~13:19 구간 이벤트
    매 5분마다 새 파일이 자동으로 생성되며, Airflow DAG(5분 스케줄)가
    현재 버킷 파일을 제외한 이전 파일들을 배치 처리합니다.
    """
    # [운영용] UTC 기준 YYYYMMDDHH 포맷 (시간 단위)
    # hour_str = datetime.datetime.utcnow().strftime("%Y%m%d%H")

    # [연습용] UTC 기준 YYYYMMDDHHMM 포맷 (5분 버킷 단위)
    now = datetime.datetime.utcnow()
    minute_bucket = (now.minute // 5) * 5          # 예: 분 13 → 버킷 10
    hour_str = now.strftime("%Y%m%d%H") + f"{minute_bucket:02d}"

    os.makedirs(DATALAKE_PATH, exist_ok=True)
    filepath = os.path.join(DATALAKE_PATH, f"events_{hour_str}.jsonl")

    # JSONL(JSON Lines) 구조로 파일 끝에 한 줄씩 추가
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, ensure_ascii=False) + '\n')

    return hour_str

if __name__ == "__main__":
    print("Kafka 컨슈머를 초기화합니다...")
    consumer = get_kafka_consumer()
    print(f"[{KAFKA_TOPIC}] 토픽 구독 시작. 이벤트를 DataLake에 적재합니다...")

    try:
        for message in consumer:
            event = message.value
            hour_str = append_to_datalake(event)
            print(f"[Consumer] {event['timestamp']} | {event['event_type']:>9} | → events_{hour_str}.jsonl")

    except KeyboardInterrupt:
        print("\n컨슈머를 종료합니다.")
    finally:
        consumer.close()
        print("Kafka 컨슈머 연결이 안전하게 종료되었습니다.")
