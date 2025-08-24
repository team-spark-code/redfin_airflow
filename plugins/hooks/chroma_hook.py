# plugins/hooks/chroma_hook.py
# -*- coding: utf-8 -*-
"""
Chroma Hook
- Chroma(로컬 퍼시스턴스) 컬렉션을 열거나 생성하는 헬퍼
- LangChain의 Chroma 래퍼를 사용 (권장: 동일 버전 유지)
- Airflow Connection이 꼭 필요하지는 않으나, 일관된 접근을 위해
  extras로 persist_directory/collection 기본값을 제공할 수 있음.

환경변수 설정 (우선순위: 환경변수 > Connection > 기본값)
- CHROMA_PERSIST_DIR: Chroma 저장소 경로
- CHROMA_COLLECTION: 기본 컬렉션 이름
- CHROMA_CACHE_ENABLED: HTTP 캐시 활성화 여부
- CHROMA_CACHE_EXPIRATION: 캐시 만료 시간 (초)

Airflow Connection 예시 (Conn ID: chroma_default)
- Conn Type: 'generic' 또는 'http' (임의)
- Host/Schema는 미사용
- Extras(JSON):
    {
      "persist_directory": "/opt/airflow/chroma",
      "collection": "ai_news_en"
    }
"""
from __future__ import annotations
from typing import Optional, Tuple, Any
from pathlib import Path
import json
import os
from dotenv import load_dotenv
load_dotenv()

from airflow.hooks.base import BaseHook
from langchain_community.vectorstores import Chroma


# 환경 변수 설정
CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "/opt/airflow/chroma")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "default")


class ChromaHook(BaseHook):
    """
    persist_directory/collection 파라미터를 중앙에서 관리하기 위한 Hook.
    - operator에서 임베딩 객체만 주입하면, 컬렉션 핸들을 돌려줌.
    - 환경변수, Airflow Connection, 기본값 순으로 설정 우선순위 적용
    """

    def __init__(self, chroma_conn_id: Optional[str] = None) -> None:
        super().__init__()
        self.conn_id = chroma_conn_id

    def _load_env_defaults(self) -> Tuple[Optional[str], Optional[str]]:
        """
        환경변수에서 기본 persist_directory/collection을 읽어온다.
        """
        persist_dir = os.getenv("CHROMA_PERSIST_DIR")
        collection = os.getenv("CHROMA_COLLECTION")
        return persist_dir, collection

    def _load_conn_defaults(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Airflow Connection에서 기본 persist_directory/collection을 읽어온다.
        """
        if not self.conn_id:
            return None, None
        conn = BaseHook.get_connection(self.conn_id)
        if not conn or not conn.extra:
            return None, None
        try:
            extra = json.loads(conn.extra)
        except Exception:
            extra = {}
        return extra.get("persist_directory"), extra.get("collection")

    def _get_defaults(self) -> Tuple[str, str]:
        """
        설정 우선순위: 환경변수 > Connection > 기본값
        """
        # 1. 환경변수 확인
        env_dir, env_col = self._load_env_defaults()
        
        # 2. Connection 확인
        conn_dir, conn_col = self._load_conn_defaults()
        
        # 3. 우선순위 적용
        persist_directory = env_dir or conn_dir or "/opt/airflow/chroma"
        collection = env_col or conn_col or "default"
        
        return persist_directory, collection

    def get_vectorstore(
        self,
        embedding_fn: Any,
        persist_directory: Optional[str] = None,
        collection: Optional[str] = None,
    ) -> Chroma:
        """
        LangChain Chroma VectorStore 인스턴스를 반환.
        - persist_directory/collection 인자가 없으면 환경변수 > Connection > 기본값 순으로 사용.
        - persist_directory가 존재하지 않으면 생성.
        """
        default_dir, default_col = self._get_defaults()
        persist_directory = str(persist_directory or default_dir)
        collection = collection or default_col

        p = Path(persist_directory)
        p.mkdir(parents=True, exist_ok=True)

        self.log.info("Opening Chroma collection '%s' at %s", collection, p)
        self.log.info("Using persist_directory: %s", persist_directory)

        vs = Chroma(
            collection_name=collection,
            embedding_function=embedding_fn,
            persist_directory=persist_directory,
        )
        return vs

    def get_collection_info(self) -> dict:
        """
        현재 설정된 Chroma 컬렉션 정보를 반환.
        """
        persist_directory, collection = self._get_defaults()
        return {
            "persist_directory": persist_directory,
            "collection": collection,
            "source": "environment" if os.getenv("CHROMA_PERSIST_DIR") else 
                     "connection" if self.conn_id else "default"
        }
