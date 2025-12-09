# dags/rss/ingest.py
"""
RSS → JSONL(원문+HTML) → Docs(JSONL, 청킹 완료) 단계까지 수행하는 얇은 DAG.
- 크롤링: Scrapy 실행 (plugins/operators/scrapy_operators.py)
- 전처리: trafilatura/ftfy/청킹 (plugins/operators/preprocess_operators.py)
- 산출 경로: /opt/airflow/data/rss_ingest/{{ ts }}/
"""
from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.utils import timezone

import sys
sys.path.append('/opt/airflow/plugins')

from utils.config import load_config
from operators.scrapy_operators import ScrapyExportJsonlOperator
from operators.preprocess_operators import JsonlToDocsOperator

# ---- 설정 ----
cfg = load_config()

KST = pendulum.timezone("Asia/Seoul")

# 스케줄(평일 6회 예시). 필요 시 주말용 별도 DAG 분리 권장
SCHEDULE_CRON = "0 1,5,9,13,17,21 * * 1-5" 

default_args = {
    "owner": "redfin",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 8, 1, tz=KST),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_ingest",
    description="RSS 수집 → 전처리/청킹 문서 생성",
    start_date=pendulum.datetime(2025, 8, 25, tz=KST),
    schedule=SCHEDULE_CRON, # 평일 1시, 5시, 9시, 13시, 17시, 21시
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rss", "ingest", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 산출 경로: /opt/airflow/data/rss_ingest/{{ ts }}/
    # Jinja 템플릿은 Operator 내부에서 그대로 사용됨
    out_dir = f"{cfg['data_root']}/rss_ingest/{{{{ ts }}}}"
    articles_jsonl = f"{out_dir}/articles.jsonl"
    docs_jsonl = f"{out_dir}/articles.docs.jsonl"

    crawl_rss = ScrapyExportJsonlOperator(
        task_id="crawl_rss",
        scrapy_project="{{ var.value.SCRAPY_DIR or '/opt/airflow/redfin_scraper' }}",
        spider="rss_article_spider",   # 사용자가 만든 스파이더 이름
        outfile=articles_jsonl,
        per_request_delay=cfg["rss"]["per_request_delay"],
        concurrent_requests=cfg["rss"]["concurrent_requests"],
        doc="Scrapy 프로젝트의 rss_article_spider를 실행해 JSONL을 생성합니다.",
    )

    to_docs = JsonlToDocsOperator(
        task_id="rss_to_docs",
        input_jsonl=articles_jsonl,
        output_jsonl=docs_jsonl,
        min_chars=200,         # 품질 컷오프(짧은 문서 제외)
        ascii_ratio_min=0.9,   # 영어 휴리스틱(ASCII 비율)
        doc="trafilatura/ftfy로 본문 정제 후 LangChain 문서(JSONL)로 변환합니다.",
    )

    end = EmptyOperator(task_id="end")

    chain(start, crawl_rss, to_docs, end)
