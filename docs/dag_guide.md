# DAG 작성 가이드

## DAG 작성 원칙

### 1. DAG는 얇게 유지
- DAG는 작업 순서와 의존성만 정의
- 비즈니스 로직은 Operator나 `plugins/libs/`로 분리

### 2. 명확한 네이밍
- 파일명: `{domain}_{purpose}.py` (예: `ingest.py`, `to_vector.py`)
- dag_id: 파일명과 일치하거나 의미 있는 이름
- task_id: 동사로 시작 (예: `crawl_rss`, `preprocess_docs`)

### 3. DAG Factory 활용
```python
from dags.dag_factory import build_dag

dag = build_dag(
    dag_id="rss_ingest",
    schedule="0 1,5,9,13,17,21 * * 1-5",
    tags=["rss", "ingest"]
)
```

### 4. 설정 외부화
```python
from plugins.utils.config import load_config

cfg = load_config()
data_root = cfg["data_root"]
```

## DAG 템플릿

```python
# dags/rss/example.py
from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

from plugins.utils.config import load_config
from plugins.operators.example_operator import ExampleOperator

cfg = load_config()
KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "redfin",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 1, 1, tz=KST),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="example",
    description="예제 DAG",
    start_date=pendulum.datetime(2025, 1, 1, tz=KST),
    schedule="0 0 * * *",  # 매일 자정
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    example_task = ExampleOperator(
        task_id="example_task",
        # 파라미터
    )
    
    end = EmptyOperator(task_id="end")
    
    chain(start, example_task, end)
```

## 태스크 의존성

### 기본 패턴
```python
# 순차 실행
task1 >> task2 >> task3

# 병렬 실행
[task1, task2] >> task3

# chain 사용 (가독성 향상)
from airflow.models.baseoperator import chain
chain(start, task1, task2, task3, end)
```

## 에러 처리

### 재시도 설정
```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}
```

### 알림 설정
```python
default_args = {
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["admin@example.com"],
}
```

## 테스트

### 로컬 테스트
```python
# dags/rss/local_test.py
# 로컬 개발용 테스트 DAG
```

### 단위 테스트
```python
# tests/dags/test_rss_ingest.py
def test_dag_loaded():
    from dags.rss.ingest import dag
    assert dag is not None
    assert len(dag.tasks) > 0
```

## 모범 사례

1. **catchup=False** 사용 (기본값)
   - 과거 데이터 백필은 별도 DAG로 처리

2. **max_active_runs=1** 설정
   - 동시 실행 방지로 리소스 관리

3. **태그 활용**
   - DAG 그룹화 및 필터링
   - 예: `["rss", "ingest", "etl"]`

4. **설명 추가**
   - `description` 파라미터로 DAG 목적 명시

5. **XCom 사용 최소화**
   - 작은 데이터만 전달
   - 큰 데이터는 파일 경로만 전달

