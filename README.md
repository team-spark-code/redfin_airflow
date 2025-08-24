# Redfin Airflow

### 설정 관리 원칙
- 단계적 로딩: base.yml -> {AIRFLOW_ENV}.yaml(dev/prod) -> 환경변수
- Airflow Connections로 비밀 관리(예: mariadb_default), 일반 파라미터만 YAML에 둠
- 데이터 산출 경로는 규칙화:
    - `/{AIRFLOW_HOME}/data/{dag_id}/{execution_date}/`
    - `/opt/airflow/data/rss_ingest/2025-08-24T05-00-00Z/articles.jsonl`

### 실행
#### 1. 로컬 실행
```bash
uv pip install apache-airflow==2.10.5
which airflow

# metadata DB 생성 (기본 DB: sqlite / 기본설정: airflow.cfg)
airflow db init

# 유저 계정 생성
airflow users create \
    --username redfin \
    --firstname red \
    --lastname fin \
    --role Admin \
    --password Redfin7620! \
    --email skybluee0612@gmail.com

# Webserver 포트 설정 및 기동 (기본: 0.0.0.0:8080)
# airflow webserver # cfg 내 default 설정값으로 실행
airflow webserver --port 8085

# Scheduler 실행
airflow scheduler
```


### 프로젝트 레이아웃
```
airflow/
├─ dags/
│  ├─ rss/
│  │  ├─ rss_ingest_dag.py             # RSS→(HTML/본문)→JSONL 산출 DAG(얇게)
│  │  ├─ rss_to_vector_dag.py          # 전처리→청킹→임베딩→Qdrant/FAISS
│  │  └─ rss_report_{daily,weekly,monthly}.py
│  ├─ __init__.py
│  ├─ dag_factory.py                    # 공통 DAG 빌더(태그/리트라이/스케줄)
│  └─ datasets.py                       # Dataset 정의(리포트 트리거)
├─ include/
│  ├─ configs/
│  │  ├─ base.yaml                      # 공통 기본값
│  │  ├─ dev.yaml                       # 개발 환경 오버레이
│  │  └─ prod.yaml                      # 운영 환경 오버레이
│  ├─ rss_feeds/
│  │  └─ ai_feeds.yaml                  # 언론/테크/랩/대학 RSS 목록
│  └─ sql/
│     ├─ ddl_articles.sql
│     ├─ ddl_keywords.sql
│     └─ queries_report.sql
├─ plugins/
│  ├─ __init__.py
│  ├─ operators/
│  │  ├─ scrapy_operators.py            # Scrapy 실행, 산출 경로 XCom push
│  │  ├─ preprocess_operators.py        # trafilatura/ftfy/bleach 전처리
│  │  └─ vectorstore_operators.py       # Qdrant/FAISS 업서트
│  ├─ hooks/
│  │  ├─ mariadb_hook.py
│  │  └─ qdrant_hook.py
│  ├─ sensors/
│  │  └─ file_ready_sensor.py
│  └─ utils/
│     ├─ text_cleaning.py               # 공백/문자정규화/언어휴리스틱
│     ├─ chunking.py                    # TextSplitter 프리셋
│     ├─ url_normalize.py               # utm/gclid 제거
│     └─ config.py                      # YAML+ENV 로딩(Pydantic)
├─ libs/                                 # 순수 파이썬 라이브러리(로직)
│  ├─ __init__.py
│  ├─ rss_pipeline/
│  │  ├─ feed_loader.py                 # feedparser 로드/ETag/Last-Modified
│  │  ├─ article_extract.py             # trafilatura extract(json)
│  │  └─ keyword_extract.py             # YAKE/KeyBERT 공통 API
│  └─ storage/
│     ├─ artifact_path.py               # /opt/airflow/data 경로 규칙
│     └─ hash_utils.py                  # url→md5(16), simhash 등
├─ tests/                                # 단위/통합 테스트(pytest)
│  └─ test_text_cleaning.py
├─ requirements.txt
├─ .env.example
└─ docker-compose.yml(있다면)

```