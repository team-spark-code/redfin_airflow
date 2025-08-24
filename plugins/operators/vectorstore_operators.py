# plugins/operators/vectorstore_operators.py
# -*- coding: utf-8 -*-
"""
Chroma 기반 벡터 스토어 인덱싱 Operator (LangChain)
- 입력: JSONL (각 라인은 {"page_content": "...", "metadata": {...}})
- 처리: 임베딩(OpenAI 또는 로컬 SBERT) → Chroma 컬렉션에 upsert
- 출력: Chroma 퍼시스턴스 디렉터리에 벡터 인덱스 파일(persist)

권장 requirements:
  chromadb
  langchain-community
  langchain-core
  langchain-openai         # (EMBEDDER=openai를 쓸 경우)
  sentence-transformers    # (EMBEDDER=local을 쓸 경우)
"""
from __future__ import annotations

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List, Dict, Any, Optional, Iterable
from pathlib import Path
import hashlib
import json

from langchain.schema import Document


def _make_id(doc: Document, idx: int) -> str:
    """
    Chroma용 문서 고유 ID 생성.
    - page_content + 일부 메타를 해시하여 안정적인 ID를 만든다.
    - 동일 콘텐츠가 재유입될 때 동일 ID가 생성 → upsert(=delete 후 add)로 중복 억제.
    """
    src = str(doc.metadata.get("source", ""))
    ts = str(doc.metadata.get("published_at", ""))
    key = f"{doc.page_content}||{src}||{ts}||{idx}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _load_docs_from_jsonl(path: Path) -> List[Document]:
    docs: List[Document] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                docs.append(Document(page_content=obj["page_content"], metadata=obj.get("metadata", {})))
            except Exception:
                continue
    return docs


class IndexDocsToChromaOperator(BaseOperator):
    """
    JSONL 문서를 Chroma 컬렉션에 인덱싱하는 Airflow Operator.

    Parameters
    ----------
    docs_jsonl : str
        입력 JSONL 경로 (Jinja 템플릿 가능: 예 `/opt/airflow/data/rss_ingest/{{ ts }}/articles.docs.jsonl`)
    persist_directory : str
        Chroma 퍼시스턴스 디렉터리 경로(컬렉션 파일 저장 위치)
    collection_name : str
        Chroma 컬렉션 이름
    embedder : str
        "openai" 또는 "local" (local은 sentence-transformers 사용)
    sentence_model : str
        EMBEDDER=local 일 때 사용할 모델명 (예: "all-MiniLM-L6-v2")
    batch_size : int
        인덱싱 배치 크기(메모리/성능 균형을 위해 200~1000 권장)
    upsert : bool
        True면 동일 ID를 미리 delete 후 add(사실상 upsert), False면 항상 add만 수행
    """

    template_fields = ("docs_jsonl", "persist_directory", "collection_name")

    @apply_defaults
    def __init__(
        self,
        docs_jsonl: str,
        persist_directory: str,
        collection_name: str,
        embedder: str = "openai",
        sentence_model: str = "all-MiniLM-L6-v2",
        batch_size: int = 500,
        upsert: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.docs_jsonl = docs_jsonl
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        self.embedder = embedder
        self.sentence_model = sentence_model
        self.batch_size = batch_size
        self.upsert = upsert

    # ---- 내부 유틸 ----
    def _build_embeddings(self):
        if self.embedder.lower() == "openai":
            # OPENAI_API_KEY는 환경변수로 읽음
            from langchain_openai import OpenAIEmbeddings
            return OpenAIEmbeddings()
        else:
            from langchain_community.embeddings import HuggingFaceEmbeddings
            return HuggingFaceEmbeddings(model_name=self.sentence_model)

    def _open_chroma(self, embeddings):
        """
        존재하는 컬렉션을 열거나, 없으면 생성한다.
        LangChain의 Chroma 래퍼를 사용하며, persist_directory를 반드시 지정한다.
        """
        from langchain_community.vectorstores import Chroma
        return Chroma(
            collection_name=self.collection_name,
            embedding_function=embeddings,
            persist_directory=self.persist_directory,
        )

    def _iter_batches(self, docs: List[Document], batch_size: int) -> Iterable[List[Document]]:
        for i in range(0, len(docs), batch_size):
            yield docs[i : i + batch_size]

    # ---- 실행 ----
    def execute(self, context):
        # 1) 경로 렌더링 및 입력 검증
        docs_path = Path(self.render_template(self.docs_jsonl, context))
        persist_dir = Path(self.render_template(self.persist_directory, context))
        collection = self.render_template(self.collection_name, context)

        if not docs_path.exists():
            raise FileNotFoundError(f"Docs JSONL not found: {docs_path}")
        persist_dir.mkdir(parents=True, exist_ok=True)

        self.log.info("Loading docs: %s", docs_path)
        docs = _load_docs_from_jsonl(docs_path)
        if not docs:
            self.log.warning("No documents to index. Skipping.")
            return str(persist_dir)

        self.log.info("Building embeddings: %s", self.embedder)
        embeddings = self._build_embeddings()

        self.log.info("Opening Chroma collection '%s' at %s", collection, persist_dir)
        vs = self._open_chroma(embeddings)

        total_added = 0
        # 2) 배치 처리(upsert: delete → add)
        for batch_idx, batch in enumerate(self._iter_batches(docs, self.batch_size), start=1):
            ids = [_make_id(doc, idx) for idx, doc in enumerate(batch)]
            self.log.info("Batch %d: size=%d", batch_idx, len(batch))

            if self.upsert:
                try:
                    # 존재하지 않는 ID가 포함되어도 삭제는 조용히 무시됨
                    vs.delete(ids=ids)
                except Exception as e:
                    # 일부 backend 버전에서 delete의 필터 동작이 제한될 수 있음 → 경고만 남기고 계속
                    self.log.warning("Delete (upsert) failed: %s", e)

            try:
                vs.add_documents(batch, ids=ids)
            except Exception as e:
                # add 실패 시, 문제되는 도큐먼트 로그 후 계속
                self.log.exception("Failed to add batch %d: %s", batch_idx, e)
                # 필요 시 재시도 로직을 추가할 수 있음
                continue

            total_added += len(batch)

        # 3) 디스크에 persist
        try:
            vs.persist()
        except Exception as e:
            # LangChain Chroma는 add 시점에 이미 flush할 수 있으나,
            # 명시적 persist를 시도하고 실패해도 오류로 처리하지 않고 경고만 남김.
            self.log.warning("Chroma persist warning: %s", e)

        self.log.info("Indexed %d documents into Chroma '%s' (dir=%s)", total_added, collection, persist_dir)
        # downstream에서 사용할 수 있도록 persist 디렉터리 경로 반환
        return str(persist_dir)
