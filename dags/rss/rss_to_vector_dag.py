# dags/rss_to_vector_dag.py
import os, json, re, datetime as dt
from pathlib import Path
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ftfy import fix_text
import bleach
from langchain.schema import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ==== 환경 ====
TZ = pendulum.timezone("Asia/Seoul")
DATA_DIR = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
SCRAPY_DIR = Path(os.getenv("SCRAPY_DIR", "/opt/airflow/redfin_scraper"))

# 임베딩/벡터DB
EMBEDDER = os.getenv("EMBEDDER", "openai")  # "openai"|"local"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
SENTENCE_MODEL = os.getenv("SENTENCE_MODEL", "all-MiniLM-L6-v2")
CHROMA_DIR = os.getenv("CHROMA_DIR", "/opt/airflow/chroma")  # Chroma 저장소 경로
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "ai_news_en")

def _outfile_template() -> str:
    return f"articles_{{{{ ds_nodash }}}}_{{{{ ts_nodash[9:13] }}}}.jsonl"

def _clean_text(s: str) -> str:
    s = fix_text(s or "")
    s = bleach.clean(s, tags=[], attributes={}, strip=True)
    return re.sub(r"\s+", " ", s).strip()

def _is_en(text: str) -> bool:
    if not text: return False
    ascii_ratio = sum(1 for ch in text if ord(ch) < 128) / max(1, len(text))
    return ascii_ratio > 0.9

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1200, chunk_overlap=150, separators=["\n\n", "\n", ". ", " "]
)

def preprocess_to_docs(ti):
    in_path = Path(ti.xcom_pull(task_ids="crawl_articles"))
    if not in_path.exists():
        raise FileNotFoundError(in_path)
    out_docs = in_path.with_suffix(".docs.jsonl")

    with in_path.open("r", encoding="utf-8") as fin, out_docs.open("w", encoding="utf-8") as fout:
        for line in fin:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = (item.get("url") or "").strip()
            html = item.get("html") or item.get("content") or ""
            title = _clean_text(item.get("title") or "")
            author = _clean_text(item.get("author") or "")
            source = (item.get("source") or "").strip()
            published_raw = (item.get("published_at") or item.get("published") or "").strip()

            text = _clean_text(html)
            if len(text) < 200 or not _is_en(text):
                continue

            published_at = None
            if published_raw:
                try:
                    published_at = dt.datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
                except Exception:
                    pass

            meta = {
                "source": url,
                "title": title[:512],
                "author": author[:128],
                "hostname": source,
                "lang": "en",
                "published_at": published_at.isoformat() if published_at else None,
            }
            for c in splitter.split_text(text):
                fout.write(json.dumps({"page_content": c, "metadata": meta}, ensure_ascii=False) + "\n")

    ti.xcom_push(key="docs_path", value=str(out_docs))

def index_docs(**context):
    from langchain_community.vectorstores import Chroma

    docs_path = Path(context["ti"].xcom_pull(task_ids="preprocess_to_docs", key="docs_path"))
    if not docs_path.exists():
        return

    # 1) 문서 로드
    records = []
    with docs_path.open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            records.append(Document(page_content=o["page_content"], metadata=o["metadata"]))

    # 2) 임베딩
    if EMBEDDER == "openai":
        from langchain_openai import OpenAIEmbeddings
        embedding = OpenAIEmbeddings()
    else:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        embedding = HuggingFaceEmbeddings(model_name=SENTENCE_MODEL)

    # 3) Chroma upsert
    vs = Chroma(
        collection_name=CHROMA_COLLECTION,
        embedding_function=embedding,
        persist_directory=CHROMA_DIR,
    )
    vs.add_documents(records)
    vs.persist()

with DAG(
    dag_id="rss_to_vector_dag",
    start_date=pendulum.now(TZ).subtract(days=1),
    schedule_interval="0 1,5,9,13,17,21 * * *",  # 하루 6회
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
    tags=["rss", "rag", "chromadb"],
) as dag:

    outfile = str(DATA_DIR / _outfile_template())

    crawl_articles = BashOperator(
        task_id="crawl_articles",
        cwd=str(SCRAPY_DIR),
        bash_command=(
            "set -euo pipefail; "
            "OUTFILE=\"" + outfile + "\"; "
            "mkdir -p \"$(dirname \"$OUTFILE\")\"; "
            "scrapy crawl article_extractor "
            "-s LOG_LEVEL=INFO "
            f"-s FEEDS='{{\"{outfile}\": {{\"format\": \"jsonlines\"}}}}' "
            "&& echo \"$OUTFILE\""
        ),
        do_xcom_push=True,
        env={"PYTHONUNBUFFERED": "1"},
    )

    to_docs = PythonOperator(
        task_id="preprocess_to_docs",
        python_callable=preprocess_to_docs,
    )

    index = PythonOperator(
        task_id="index_to_chromadb",
        python_callable=index_docs,
    )

    crawl_articles >> to_docs >> index
