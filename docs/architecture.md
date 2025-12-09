# RedFin Airflow 아키텍처

## 개요

RedFin Airflow는 AI 뉴스 수집 및 처리 파이프라인을 관리하는 워크플로우 오케스트레이션 시스템입니다.

## 프로젝트 구조

```
redfin_airflow/
├── dags/                    # DAG 정의 파일
│   ├── rss/                # RSS 도메인 DAG
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
├── docs/                    # 문서
└── scripts/                 # 배포/실행 스크립트
```

## 핵심 원칙

### 1. DAG는 얇게 유지
- DAG는 오케스트레이션만 담당
- 비즈니스 로직은 `plugins/libs/`로 분리
- 재사용 가능한 Operator/Hook 활용

### 2. 도메인별 그룹화
- 관련 DAG를 하위 디렉토리로 그룹화
- 예: `dags/rss/` (RSS 관련 DAG)
- 향후 확장: `dags/ml/`, `dags/analytics/` 등

### 3. 설정 외부화
- 하드코딩 지양
- YAML 계층 구조: `base.yaml` → `dev.yaml` → `prod.yaml`
- 환경변수 오버라이드 지원

### 4. 재사용 가능한 컴포넌트
- 커스텀 Operator/Hook/Sensor로 추상화
- 여러 DAG에서 재사용 가능
- 테스트 용이

## 데이터 흐름

### RSS 수집 파이프라인
```
RSS Feed → Scrapy → JSONL → 전처리 → 청킹 → 벡터화 → Vector DB
```

### 주요 DAG

1. **rss_ingest** (`dags/rss/ingest.py`)
   - RSS 피드 수집
   - HTML 본문 추출
   - JSONL 형식으로 저장

2. **rss_to_vector** (`dags/rss/to_vector.py`)
   - 전처리 및 청킹
   - 임베딩 생성
   - Vector DB 저장

3. **rss_report_*** (`dags/rss/report_*.py`)
   - 일/주/월 리포트 생성
   - 통계 집계

## 설정 관리

### 계층 구조
1. `include/configs/base.yaml` - 공통 기본값
2. `include/configs/{AIRFLOW_ENV}.yaml` - 환경별 오버레이
3. 환경변수 - 최종 오버라이드

### 데이터 경로 규칙
```
{data_root}/{dag_id}/{execution_date}/
예: /opt/airflow/data/rss_ingest/2025-08-24T05-00-00Z/articles.jsonl
```

## 확장성

- 새로운 도메인 추가: `dags/{domain}/` 디렉토리 생성
- 새로운 Operator 추가: `plugins/operators/`에 구현
- 새로운 Hook 추가: `plugins/hooks/`에 구현

