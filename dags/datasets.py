# dags/datasets.py
# -*- coding: utf-8 -*-
"""
Airflow Datasets 선언 모듈

이 파일은 DAG 간 느슨한 결합(loose coupling)을 위해 사용하는
Airflow Dataset 객체들을 **단일 위치**에서 정의합니다.

권장 패턴
---------
1) **생산(emit)** 쪽 태스크에서 `outlets=[DATASET_X]` 를 지정
2) **소비(consume)** 쪽 DAG 정의에서 `schedule=[DATASET_X]` 로 의존
   → 파일 경로 등 구체 값 전달은 XCom/규칙화된 디렉터리로 처리

왜 이렇게 하나요?
-----------------
- Dataset의 URI는 템플릿이 적용되지 않으므로, 파티션(예: {{ ts }})을
  URI에 직접 넣는 방식은 맞지 않습니다.
- 대신 "논리적 이벤트"를 표현하는 정적 URI를 사용하고,
  실제 산출물 경로는 규칙화된 디렉터리 구조(/opt/airflow/data/...)로
  관리합니다. (경로 생성은 아래 `make_data_paths()` 참고)

사용 예
-------
from dags.datasets import (
    DS_RSS_ARTICLES, DS_RSS_DOCS, DS_VECTORSTORE_CHROMA, make_data_paths
)

with DAG(..., schedule=[DS_RSS_DOCS]) as dag:
    ...

crawl_task = BashOperator(..., outlets=[DS_RSS_ARTICLES])
to_docs_task = PythonOperator(..., outlets=[DS_RSS_DOCS])

필요 Airflow 버전
----------------
- Airflow 2.4+ : Dataset 기본 지원
"""

from __future__ import annotations

from airflow.datasets import Dataset
from typing import Dict
from pathlib import Path

# ---------------------------------------------------------------------------
# 1) 논리 Dataset 정의
#    - URI는 "dataset://<domain>/<name>" 형태로 간단·가독성 있게 지정
# ---------------------------------------------------------------------------

#: RSS 크롤링이 원문 JSONL(HTML 포함)을 산출했음을 알리는 이벤트
DS_RSS_ARTICLES = Dataset("dataset://rss/articles")

#: RSS 원문을 전처리/청킹하여 LangChain 문서(JSONL)를 산출했음을 알리는 이벤트
DS_RSS_DOCS = Dataset("dataset://rss/docs")

#: 벡터 스토어(Chroma)에 인덱싱이 반영되었음을 알리는 이벤트
DS_VECTORSTORE_CHROMA = Dataset("dataset://vectorstore/chroma")

#: 리포트 산출(선택): 일간/주간/월간
DS_REPORT_DAILY = Dataset("dataset://report/daily")
DS_REPORT_WEEKLY = Dataset("dataset://report/weekly")
DS_REPORT_MONTHLY = Dataset("dataset://report/monthly")


# ---------------------------------------------------------------------------
# 2) 산출물 경로 규칙 도우미
#    - datasets는 "신호"만 담당하고, 실제 파일 경로는 아래 규칙을 사용
#    - cfg는 plugins.utils.config.load_config() 반환 dict를 기대
# ---------------------------------------------------------------------------

def make_data_paths(dag_id: str, ts_iso: str, cfg: Dict) -> Dict[str, str]:
    """
    표준 산출 경로를 생성합니다.

    Parameters
    ----------
    dag_id : str
        산출 주체 DAG ID (예: 'rss_ingest')
    ts_iso : str
        실행 시각 ISO 문자열 (Airflow Jinja '{{ ts }}' 값 사용을 권장)
        예) '2025-08-24T01:00:00+00:00'
    cfg : dict
        plugins.utils.config.load_config()로 로드한 설정

    Returns
    -------
    dict
        {
          "dir": "<root>/<dag_id>/<ts>/",
          "articles_jsonl": ".../articles.jsonl",
          "docs_jsonl": ".../articles.docs.jsonl",
          "chroma_dir": "<chroma persist dir>"
        }
    """
    data_root = Path(cfg.get("data_root", "/opt/airflow/data"))
    base = data_root / dag_id / ts_iso
    chroma_dir = cfg.get("vectorstore", {}).get("chroma_dir", "/opt/airflow/chroma")

    return {
        "dir": str(base),
        "articles_jsonl": str(base / "articles.jsonl"),
        "docs_jsonl": str(base / "articles.docs.jsonl"),
        "chroma_dir": chroma_dir,
    }


__all__ = [
    "DS_RSS_ARTICLES",
    "DS_RSS_DOCS",
    "DS_VECTORSTORE_CHROMA",
    "DS_REPORT_DAILY",
    "DS_REPORT_WEEKLY",
    "DS_REPORT_MONTHLY",
    "make_data_paths",
]
