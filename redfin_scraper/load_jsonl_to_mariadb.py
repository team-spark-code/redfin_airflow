#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, hashlib, math
from datetime import datetime
from typing import Iterator, Dict, Any, List, Optional

from sqlalchemy import create_engine, Table, Column, BigInteger, Integer, String, Text, JSON as SA_JSON
from sqlalchemy import DateTime, MetaData, func
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine

# ---- 설정: 환경변수로 관리 ----
DB_USER     = os.getenv("MARIADB_USER", "redfin")
DB_PASS     = os.getenv("MARIADB_PASSWORD", "Redfin7620!")
DB_HOST     = os.getenv("MARIADB_HOST", "localhost")
DB_PORT     = int(os.getenv("MARIADB_PORT", "3306"))
DB_NAME     = os.getenv("MARIADB_DB", "redfin")

JSONL_PATH  = os.getenv("JSON_PATH", "all_entries_20250825_025249.jsonl")  # 예: /data/all_entries_20250825_025249.jsonl
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "1000"))

if not JSONL_PATH:
    print("ERROR: JSONL_PATH 환경변수를 지정하세요", file=sys.stderr)
    sys.exit(2)

def file_sha256(path: str, chunk_size: int = 2**20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b: break
            h.update(b)
    return h.hexdigest()

def parse_pub_date(v: Optional[str]) -> Optional[datetime]:
    if not v: return None
    # RFC3339/ISO8601 대강 처리(정교한 파서는 dateutil.parser 권장)
    try:
        # '2025-08-14T13:00:00+00:00' → timezone drop(로컬/UTC 정규화는 운영 정책에 맞춰 조정)
        return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def iter_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line: 
                yield {"__line_no__": line_no, "__error__": "EMPTY_LINE"}
                continue
            try:
                obj = json.loads(line)
                obj["__line_no__"] = line_no
                yield obj
            except Exception as e:
                yield {"__line_no__": line_no, "__error__": f"JSON_PARSE_ERROR: {e}"}

def build_rows(src_file: str, fhash: str, objs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for o in objs:
        line_no = o.get("__line_no__")
        if "__error__" in o:
            # 에러 라인은 raw_json에 최소 정보로라도 남길지 정책 결정(여기선 스킵)
            continue

        # 원본 필드 매핑(있을 때만)
        guid   = o.get("guid")
        source = o.get("source")
        title  = o.get("title")
        link   = o.get("link")
        pub    = o.get("published") or o.get("pub_date")  # 다양한 키 대응
        pub_dt = parse_pub_date(pub) if isinstance(pub, str) else None
        author = o.get("authors") or o.get("author")
        summary= o.get("summary")
        content_text = (o.get("content_text")
                        or o.get("content") 
                        or o.get("html_text")
                        or None)

        rows.append({
            "src_file": src_file,
            "file_sha256": fhash,
            "line_no": line_no,
            "raw_json": o,                 # 원본을 그대로 JSON에 저장
            "guid": guid,
            "source": source,
            "title": title,
            "link": link,
            "pub_date": pub_dt,
            "author": author,
            "summary": summary,
            "content_text": content_text,
        })
    return rows

def get_table(metadata: MetaData) -> Table:
    # DDL은 DB에 이미 생성되어 있다는 가정(없다면 metadata.create_all 사용 가능)
    return Table("jsonl_ingest", metadata, autoload_with=engine)

def upsert_rows(engine: Engine, table: Table, rows: List[Dict[str, Any]]):
    if not rows: return
    stmt = mysql_insert(table).values(rows)
    # 고유키: (file_sha256, line_no) 기준으로 idempotent
    update_cols = {
        # 원본을 덮어쓰고 싶지 않다면 필요 컬럼만 선택
        "raw_json": stmt.inserted.raw_json,
        "guid": stmt.inserted.guid,
        "source": stmt.inserted.source,
        "title": stmt.inserted.title,
        "link": stmt.inserted.link,
        "pub_date": stmt.inserted.pub_date,
        "author": stmt.inserted.author,
        "summary": stmt.inserted.summary,
        "content_text": stmt.inserted.content_text,
        "ingested_at": func.current_timestamp(),
    }
    ondup = stmt.on_duplicate_key_update(**update_cols)
    with engine.begin() as conn:
        conn.execute(ondup)

# ---- main ----
engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True, pool_size=10, max_overflow=20, json_serializer=lambda x: json.dumps(x, ensure_ascii=False)
)
metadata = MetaData()
# (스키마가 없다면) 아래 주석 해제 후 최초 1회 테이블 생성 로직 추가 가능
# from sqlalchemy import Table, Column, BigInteger, Integer, String, DateTime, JSON as SA_JSON, Text
# Table(...); metadata.create_all(engine)

src_file = os.path.abspath(JSONL_PATH)
fhash = file_sha256(src_file)

buf: List[Dict[str, Any]] = []
for obj in iter_jsonl(src_file):
    buf.append(obj)
    if len(buf) >= BATCH_SIZE:
        rows = build_rows(src_file, fhash, buf)
        upsert_rows(engine, get_table(metadata), rows)
        buf.clear()

# 잔여 배치
if buf:
    rows = build_rows(src_file, fhash, buf)
    upsert_rows(engine, get_table(metadata), rows)
