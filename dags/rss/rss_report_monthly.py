# -*- coding: utf-8 -*-
import os, csv, pathlib
import pendulum
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_USER = os.getenv("MARIADB_USER", "redfin")
DB_PASS = os.getenv("MARIADB_PASSWORD", "Redfin7620!")
DB_NAME = os.getenv("MARIADB_DATABASE", "redfin")
DB_HOST = os.getenv("MARIADB_HOST", "localhost")
DB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
REPORT_DIR = pathlib.Path(os.getenv("REPORT_DIR_MONTHLY", "/opt/airflow/data/rss_report/monthly"))

def gen_monthly_report():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    eng = create_engine(URL.create(
        "mysql+pymysql",
        username=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ), pool_pre_ping=True)

    with eng.begin() as conn:
        conn.exec_driver_sql("SET time_zone = '+09:00';")
        # 지난달 범위: @m0(지난달1일 00:00) ~ @m1(이번달1일 00:00)
        conn.exec_driver_sql("SET @m1 = DATE_FORMAT(CURDATE(), '%Y-%m-01');")
        conn.exec_driver_sql("SET @m0 = DATE_SUB(@m1, INTERVAL 1 MONTH);")

        n_articles = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM articles "
            "WHERE published_at >= @m0 AND published_at < @m1;"
        ).fetchone()[0]

        top_sources = conn.exec_driver_sql(
            "SELECT source, COUNT(*) AS cnt FROM articles "
            "WHERE published_at >= @m0 AND published_at < @m1 "
            "GROUP BY source ORDER BY cnt DESC LIMIT 20;"
        ).fetchall()

        top_keywords = conn.exec_driver_sql(
            "SELECT ak.keyword, COUNT(*) AS freq, ROUND(AVG(ak.score),4) AS avg_score "
            "FROM article_keywords ak "
            "JOIN articles a ON a.id = ak.article_id "
            "WHERE a.published_at >= @m0 AND a.published_at < @m1 "
            "GROUP BY ak.keyword "
            "ORDER BY freq DESC, avg_score ASC "
            "LIMIT 50;"
        ).fetchall()

        weekly = conn.exec_driver_sql(
            "SELECT DATE_FORMAT(published_at, '%x-%v') AS year_week, COUNT(*) AS cnt "
            "FROM articles "
            "WHERE published_at >= @m0 AND published_at < @m1 "
            "GROUP BY year_week ORDER BY year_week;"
        ).fetchall()

        prev = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM articles "
            "WHERE published_at >= DATE_SUB(@m0, INTERVAL 1 MONTH) "
            "  AND published_at < @m0;"
        ).fetchone()[0]
        growth = ( (n_articles - prev) / prev * 100.0 ) if prev else None

    ym_prev = pendulum.now("Asia/Seoul").subtract(months=1).format("YYYY-MM")
    out = REPORT_DIR / f"{ym_prev}_summary.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerow(["month", ym_prev])
        w.writerow(["articles", n_articles])
        w.writerow(["mom_growth_percent", f"{growth:.2f}" if growth is not None else "NA"])
        w.writerow([])

        w.writerow(["source", "count"])
        for s, c in top_sources:
            w.writerow([s, c])
        w.writerow([])

        w.writerow(["keyword", "freq", "avg_score_yake"])
        for k, freq, score in top_keywords:
            w.writerow([k, freq, score])
        w.writerow([])

        w.writerow(["year_week", "count"])
        for yw, c in weekly:
            w.writerow([yw, c])

with DAG(
    dag_id="rss_monthly",
    start_date=pendulum.now("Asia/Seoul").subtract(months=1),
    schedule_interval="30 9 1 * *",  # 매월 1일 09:30 KST
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow"},
    tags=["rss", "report", "monthly"],
) as dag:
    PythonOperator(
        task_id="gen_monthly_report",
        python_callable=gen_monthly_report,
    )
