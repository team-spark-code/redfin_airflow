# plugins/operators/preprocess_operators.py
# -*- coding: utf-8 -*-
"""
JSONL(원문 HTML 포함)을 읽어:
- trafilatura로 본문/메타 추출
- ftfy/정규식으로 텍스트 정리
- 영어 휴리스틱/길이 컷오프 적용
- LangChain Document 포맷(JSONL)로 청킹 저장
"""
from __future__ import annotations
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional
from pathlib import Path
import json
import re
import datetime as dt

import trafilatura
from ftfy import fix_text
from langchain_text_splitters import RecursiveCharacterTextSplitter


def _clean_text(s: str) -> str:
    """유니코드/공백 정리(HTML 제거는 trafilatura에서 처리됨)."""
    s = fix_text(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_english_by_ascii_ratio(text: str, min_ratio: float = 0.9) -> bool:
    """ASCII 문자 비율로 간단한 영어 판별 휴리스틱."""
    if not text:
        return False
    ratio = sum(1 for ch in text if ord(ch) < 128) / max(1, len(text))
    return ratio >= min_ratio


class JsonlToDocsOperator(BaseOperator):
    """
    Parameters
    ----------
    input_jsonl : str
        입력 JSONL 경로 (각 라인은 {url, title, author, published_at, html, ...})
    output_jsonl : str
        출력 JSONL 경로 (각 라인은 {"page_content": "...", "metadata": {...}})
    min_chars : int
        본문 길이 컷오프(최소 글자수 미만 문서 제외)
    ascii_ratio_min : float
        영어 휴리스틱 기준 (기본 0.9)
    chunk_size : int
        청크 크기(문자 기준; 토큰 기준이 필요하면 TokenTextSplitter 사용)
    chunk_overlap : int
        청크 오버랩
    """

    template_fields = ("input_jsonl", "output_jsonl")

    @apply_defaults
    def __init__(
        self,
        input_jsonl: str,
        output_jsonl: str,
        min_chars: int = 200,
        ascii_ratio_min: float = 0.9,
        chunk_size: int = 1200,
        chunk_overlap: int = 150,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input_jsonl = input_jsonl
        self.output_jsonl = output_jsonl
        self.min_chars = min_chars
        self.ascii_ratio_min = ascii_ratio_min
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", ". ", " "],
        )

    def _parse_datetime(self, s: str) -> Optional[dt.datetime]:
        """ISO8601 형태 우선으로 게시일 파싱(실패 시 None)."""
        if not s:
            return None
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    def execute(self, context):
        # 템플릿 렌더
        inp = Path(self.render_template(self.input_jsonl, context))
        outp = Path(self.render_template(self.output_jsonl, context))
        outp.parent.mkdir(parents=True, exist_ok=True)

        kept_docs, written_chunks = 0, 0

        # 입력 파일 존재 체크
        if not inp.exists():
            raise FileNotFoundError(f"Input JSONL not found: {inp}")

        with inp.open("r", encoding="utf-8") as fin, outp.open("w", encoding="utf-8") as fout:
            for line in fin:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue

                url = (item.get("url") or "").strip()
                raw_html = item.get("html") or ""
                if not url or not raw_html:
                    continue

                # 1) trafilatura로 본문/메타 추출
                extracted = trafilatura.extract(
                    raw_html,
                    include_comments=False,
                    include_images=False,
                    include_links=False,
                    output="json",
                    with_metadata=True,
                    favor_precision=True,
                )
                if not extracted:
                    continue

                j = trafilatura.utils.json_to_dict(extracted)
                text = _clean_text(j.get("text") or "")
                if len(text) < self.min_chars:
                    continue
                if not _is_english_by_ascii_ratio(text, self.ascii_ratio_min):
                    continue

                # 2) 메타 병합(RSS 항목 우선 → trafilatura 보완)
                title = _clean_text(item.get("title") or j.get("title") or "")
                author = _clean_text(item.get("author") or j.get("author") or "")
                hostname = (item.get("source") or j.get("hostname") or "").strip()
                published_raw = (item.get("published_at") or j.get("date") or "").strip()
                published_at = self._parse_datetime(published_raw)

                metadata = {
                    "source": url,
                    "title": title[:512],
                    "author": author[:128],
                    "hostname": hostname,
                    "lang": "en",
                    "published_at": published_at.isoformat() if published_at else None,
                }

                # 3) 청킹 및 저장
                for chunk in self.splitter.split_text(text):
                    rec = {"page_content": chunk, "metadata": metadata}
                    fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    written_chunks += 1
                kept_docs += 1

        self.log.info(
            "Preprocess finished. docs_kept=%d, chunks_written=%d, output=%s",
            kept_docs, written_chunks, str(outp)
        )
        # downstream에서 사용할 수 있도록 출력 경로 반환
        return str(outp)
