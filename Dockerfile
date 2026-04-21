FROM python:3.11-slim

WORKDIR /app

# 필요 패키지 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 코드 스크립트 복사 (프로듀서 + 컨슈머 둘 다 포함)
COPY log.py .
COPY consumer.py .

# 컨테이너 실행 시 파이썬 로그 스크립트 가동 (바로 출력되도록 -u 옵션 추가)
CMD ["python", "-u", "log.py"]
