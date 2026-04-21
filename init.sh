#!/bin/bash
# postgres 컨테이너 최초 기동 시 자동 실행되는 초기화 스크립트
# livedb(데이터용)와 airflow_meta(Airflow 메타DB용) 두 개의 DB를 생성합니다.
set -e

echo "PostgreSQL 초기화: airflow_meta 데이터베이스 생성 중..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow_meta;
    GRANT ALL PRIVILEGES ON DATABASE airflow_meta TO $POSTGRES_USER;
EOSQL

echo "airflow_meta 데이터베이스 생성 완료!"
