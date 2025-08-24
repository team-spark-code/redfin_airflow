# -*- coding: utf-8 -*-
import os, csv, pathlib
import pendulum
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from airflow import DAG
from airflow.operators.python import PythonOperator

# 환경
DB_USER = os.getenv("MARIADB_USER", "redfin")
DB_PASS = os.getenv("MARIADB_PASSWORD", "Redfin7620!")
DB_NAME = os.getenv("MARIADB_DATABASE", "redfin")
DB_HOST = os.getenv("MARIADB_HOST", "localhost")
DB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
REPORT_DIR = pathlib.Path(os.getenv("REPORT_DIR_DAILY", "/opt/airflow/data/rss_report/daily"))

def gen_daily_report():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    eng = create_engine(URL.create(
        "mysql+pymysql",
        username=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ), pool_pre_ping=True)

    with eng.begin() as conn:
        # KST 기준 전일 집계
        conn.exec_driver_sql("SET time_zone = '+09:00';")
        conn.exec_driver_sql("SET @d0 = DATE_SUB(CURDATE(), INTERVAL 1 DAY);")
        conn.exec_driver_sql("SET @d1 = CURDATE();")

        n_articles = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM articles "
            "WHERE published_at >= @d0 AND published_at < @d1;"
        ).fetchone()[0]

        top_sources = conn.exec_driver_sql(
            "SELECT source, COUNT(*) AS cnt FROM articles "
            "WHERE published_at >= @d0 AND published_at < @d1 "
            "GROUP BY source ORDER BY cnt DESC LIMIT 10;"
        ).fetchall()

        top_keywords = conn.exec_driver_sql(
            "SELECT ak.keyword, COUNT(*) AS freq, ROUND(AVG(ak.score),4) AS avg_score "
            "FROM article_keywords ak "
            "JOIN articles a ON a.id = ak.article_id "
            "WHERE a.published_at >= @d0 AND a.published_at < @d1 "
            "GROUP BY ak.keyword "
            "ORDER BY freq DESC, avg_score ASC "
            "LIMIT 30;"
        ).fetchall()

    d_yesterday = pendulum.now("Asia/Seoul").subtract(days=1).to_date_string()
    out = REPORT_DIR / f"{d_yesterday}_summary.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerow(["date", d_yesterday])
        w.writerow(["articles", n_articles])
        w.writerow([])

        w.writerow(["source", "count"])
        for s, c in top_sources:
            w.writerow([s, c])
        w.writerow([])

        w.writerow(["keyword", "freq", "avg_score_yake"])
        for k, freq, score in top_keywords:
            w.writerow([k, freq, score])

with DAG(
    dag_id="rss_report_daily",
    start_date=pendulum.now("Asia/Seoul").subtract(days=1),
    schedule_interval="0 8 * * *",  # 매일 08:00 KST
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow"},
    tags=["rss", "report", "daily"],
) as dag:
    PythonOperator(
        task_id="gen_daily_report",
        python_callable=gen_daily_report,
    )
