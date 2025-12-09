# 배포 가이드

## 로컬 개발 환경

### 사전 요구사항
- Python 3.10+
- Docker & Docker Compose (선택사항)

### 설치

```bash
# 가상 환경 생성
python -m venv .venv
source .venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# Airflow DB 초기화
airflow db init

# 사용자 생성
airflow users create \
    --username redfin \
    --firstname red \
    --lastname fin \
    --role Admin \
    --password <password> \
    --email <email>

# 환경 변수 설정
export AIRFLOW_HOME=$(pwd)
export AIRFLOW_ENV=dev
```

### 실행

```bash
# 웹서버 (별도 터미널)
airflow webserver --port 8085

# 스케줄러 (별도 터미널)
airflow scheduler
```

## Docker Compose 배포

### 실행

```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일 편집

# 서비스 시작
docker compose up -d

# 로그 확인
docker compose logs -f

# 서비스 중지
docker compose down
```

### 환경 변수

- `AIRFLOW_ENV`: 환경 설정 (dev/prod)
- `AIRFLOW_HOME`: Airflow 홈 디렉토리
- `DATA_ROOT`: 데이터 저장 경로
- `MONGO_URI`: MongoDB 연결 문자열
- 기타 설정은 `include/configs/` 참조

## 프로덕션 배포

### 체크리스트

- [ ] 환경 변수 설정 확인
- [ ] `include/configs/prod.yaml` 설정 확인
- [ ] Airflow Connections 설정
- [ ] Airflow Variables 설정
- [ ] 데이터 디렉토리 권한 확인
- [ ] 로그 디렉토리 설정
- [ ] 백업 전략 수립

### Airflow Connections 설정

```bash
# CLI로 설정
airflow connections add mariadb_default \
    --conn-type mysql \
    --conn-host localhost \
    --conn-port 3306 \
    --conn-login user \
    --conn-password password \
    --conn-schema database
```

### Airflow Variables 설정

```bash
# CLI로 설정
airflow variables set SCRAPY_DIR /opt/airflow/redfin_scraper
airflow variables set DATA_ROOT /opt/airflow/data
```

## 모니터링

### 로그 확인

```bash
# 웹 UI: http://localhost:8085
# 또는 로그 파일: logs/
```

### 헬스체크

```bash
# DAG 목록 확인
airflow dags list

# 특정 DAG 상태 확인
airflow dags state <dag_id> <execution_date>
```

## 트러블슈팅

### 일반적인 문제

1. **DAG가 보이지 않음**
   - `dags/` 디렉토리 경로 확인
   - DAG 파일에 문법 오류 확인
   - 스케줄러 재시작

2. **태스크 실패**
   - 로그 확인: `logs/dag_id/task_id/execution_date/`
   - 의존성 확인
   - 리소스 확인

3. **설정이 적용되지 않음**
   - 환경 변수 확인
   - 설정 파일 우선순위 확인
   - Airflow 재시작

