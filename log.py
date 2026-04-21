import os
import uuid
import random
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "web_events")

def get_kafka_producer():
    # 카프카 프로세스가 완전히 준비될 때까지 시간을 벌기 위한 재시도 로직
    max_retries = 15
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            print(f"Kafka 연결 대기 중... ({attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
    
    raise Exception("Kafka 서버에 연결할 수 없습니다. 설정을 확인해주세요.")

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
    print("Kafka 프로듀서 구동 대기 중...")
    try:
        producer = get_kafka_producer()
        print("Kafka 프로듀서 연결 성공!")
    except Exception as e:
        print(f"Kafka 프로듀서 연결 실패: {e}")
        exit(1)
        
    print("초기 데이터 약 1000건을 생성하여 Kafka로 전송(Produce)합니다... (최근 2일간 쌓인 과거 데이터)")
    inserted_count = 0
    target_count = 1000
    while inserted_count < target_count:
        events = generate_events(is_historical=True)
        for ev in events:
            # PostgreSQL에 직접 꽂는 대신, Kafka로 전송합니다.
            producer.send(KAFKA_TOPIC, ev)
            inserted_count += 1
            if inserted_count >= target_count:
                break
    # 버퍼에 있는 데이터를 카프카로 모두 밀어내기
    producer.flush()
    print(f"과거 초기 {inserted_count}건 Kafka 전송 완료!")
    
    print("이제부터 실시간 로그 생성을 시작합니다 (초당 약 2~3세트) - 종료하려면 Ctrl+C를 누르세요.")
    try:
        while True:
            # 초당 2~3회의 사용량 시뮬레이션을 위해 대기
            time.sleep(random.uniform(0.33, 0.5))
            
            events = generate_events(is_historical=False)
            for ev in events:
                producer.send(KAFKA_TOPIC, ev)
                print(f"[{ev['timestamp']}] Kafka 전송: {ev['event_type']:>9} | User: {ev['user_id']:>8} | Endpoint: {ev['details']['endpoint']}")
            producer.flush()
            print("-" * 60) # 이벤트를 쌍으로 묶어서 보기 편하게 구분선 추가
            
    except KeyboardInterrupt:
        print("\n사용자에 의해 로그 생성을 종료합니다.")
    finally:
        producer.close()
        print("Kafka 프로듀서가 안전하게 종료되었습니다.")