# -*- coding: utf-8 -*-
"""
Scrapy 스파이더들을 사용하는 간단한 DAG 예시
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

# 커스텀 Operator import
import sys
import os
sys.path.append('/opt/airflow/plugins')
from operators.scrapy_operators import ScrapyExportJsonlOperator

# DAG 설정
default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="scrapy_example",
    description="Scrapy 스파이더들을 사용한 크롤링 예시",
    schedule_interval="0 2 * * *",  # 매일 새벽 2시
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scrapy", "crawling", "example"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    # 1. RSS 피드 크롤링
    rss_crawl = ScrapyExportJsonlOperator(
        task_id="rss_crawl",
        scrapy_project="/opt/airflow/scrapy_projects/redfin_scraper",
        spider="rss_article_spider",
        outfile="/opt/airflow/data/rss/{{ ts }}/articles.jsonl",
        per_request_delay=1.0,
        concurrent_requests=4,
        doc="RSS 피드에서 기사 URL을 추출하고 각 기사를 크롤링합니다."
    )
    
    # 2. 뉴스 사이트 직접 크롤링
    news_crawl = ScrapyExportJsonlOperator(
        task_id="news_crawl",
        scrapy_project="/opt/airflow/scrapy_projects/redfin_scraper",
        spider="news_spider",
        outfile="/opt/airflow/data/news/{{ ts }}/articles.jsonl",
        per_request_delay=2.0,
        concurrent_requests=2,
        doc="뉴스 사이트를 직접 크롤링하여 최신 기사를 수집합니다."
    )
    
    end = EmptyOperator(task_id="end")
    
    # 태스크 순서: start → (rss_crawl, news_crawl) → end
    chain(start, [rss_crawl, news_crawl], end)
