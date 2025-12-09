# RedFin Airflow

AI 뉴스 수집 및 처리 파이프라인을 관리하는 Apache Airflow 워크플로우 오케스트레이션 시스템입니다.

## 📋 개요

RedFin Airflow는 RSS 피드 수집, 전처리, 벡터화, 리포트 생성 등의 데이터 파이프라인을 관리합니다. Clean Architecture 원칙을 따르며, 도메인별 DAG 그룹화와 재사용 가능한 컴포넌트를 제공합니다.

## 🏗️ 프로젝트 구조

```
redfin_airflow/
├── dags/                    # DAG 정의 파일
│   ├── rss/                # RSS 도메인 DAG
│   │   ├── ingest.py       # RSS 수집 DAG
│   │   ├── to_vector.py    # 벡터화 DAG
│   │   ├── report_daily.py
│   │   ├── report_weekly.py
│   │   └── report_monthly.py
│   ├── dag_factory.py      # 공통 DAG 빌더
│   └── datasets.py         # Dataset 정의
├── plugins/                 # 커스텀 컴포넌트
│   ├── operators/          # 커스텀 Operator
│   ├── hooks/              # 커스텀 Hook
│   ├── sensors/            # 커스텀 Sensor
│   ├── utils/              # 유틸리티 함수
│   └── libs/               # 비즈니스 로직 라이브러리
├── include/                 # 정적 파일
│   ├── configs/            # 환경별 설정
│   ├── rss_feeds/          # RSS 피드 목록
│   └── sql/                # SQL 스크립트
├── tests/                   # 테스트 코드
│   ├── dags/               # DAG 테스트
│   └── plugins/            # 플러그인 테스트
├── docs/                    # 문서
│   ├── architecture.md     # 아키텍처 문서
│   ├── dag_guide.md        # DAG 작성 가이드
│   └── deployment.md       # 배포 가이드
└── scripts/                 # 배포/실행 스크립트
```

## ⚙️ 설정 관리 원칙

- **단계적 로딩**: `base.yaml` → `{AIRFLOW_ENV}.yaml` (dev/prod) → 환경변수
- **비밀 관리**: Airflow Connections로 비밀 관리 (예: `mariadb_default`), 일반 파라미터만 YAML에 저장
- **데이터 경로 규칙화**:
    - `/{AIRFLOW_HOME}/data/{dag_id}/{execution_date}/`
    - 예: `/opt/airflow/data/rss_ingest/2025-08-24T05-00-00Z/articles.jsonl`

## 🚀 빠른 시작

### 사전 요구사항

- Python 3.10+
- Apache Airflow 2.10.5+
- MongoDB (선택사항)
- Docker & Docker Compose (선택사항)

### 설치

```bash
# 가상 환경 생성
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정
export AIRFLOW_HOME=$(pwd)
export AIRFLOW_ENV=dev

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
```

### 실행

#### 1. 로컬 실행

```bash
# 웹서버 실행 (별도 터미널)
airflow webserver --port 8085

# 스케줄러 실행 (별도 터미널)
airflow scheduler
```

웹 UI: http://localhost:8085

#### 2. Docker Compose 실행

```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일 편집

# 서비스 시작
docker compose up -d

# 로그 확인
docker compose logs -f
```

## 📚 문서

상세한 문서는 [`docs/`](./docs/) 디렉토리에서 확인할 수 있습니다:

- [🏗️ 아키텍처](./docs/architecture.md) - 프로젝트 구조 및 아키텍처 원칙
- [📝 DAG 작성 가이드](./docs/dag_guide.md) - DAG 작성 모범 사례 및 템플릿
- [🚀 배포 가이드](./docs/deployment.md) - 로컬/프로덕션 배포 방법

## 🧪 테스트

```bash
# 테스트 실행
pytest tests/

# 특정 테스트 실행
pytest tests/dags/test_rss_ingest.py

# 커버리지 포함
pytest tests/ --cov=plugins --cov=dags
```

## 🔧 주요 기능

### DAG

- **rss_ingest**: RSS 피드 수집 및 전처리
- **rss_to_vector**: 문서 벡터화 및 Vector DB 저장
- **rss_report_***: 일/주/월 리포트 생성

### 커스텀 컴포넌트

- **Operators**: Scrapy 실행, 전처리, 벡터화
- **Hooks**: MariaDB, ChromaDB 연결
- **Sensors**: 파일 준비 상태 감지
- **Utils**: 설정 관리, 텍스트 정제, URL 정규화

## 📄 라이선스

MIT License
