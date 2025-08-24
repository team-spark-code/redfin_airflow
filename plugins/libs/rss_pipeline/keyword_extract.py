# plugins/rss_pipeline/keyword_extract.py
# -*- coding: utf-8 -*-
"""
키워드 추출 유틸리티.
- YAKE(경량, 속도 빠름) 기본
- KeyBERT(품질↑, 리소스↑) 선택
- 공통 인터페이스: extract_keywords(text, method="yake"| "keybert", topk=15)

반환 형식:
[
  {"keyword": "large language models", "score": 0.0123, "method": "yake"},
  ...
]
- YAKE: score 낮을수록 중요
- KeyBERT: 원래 score는 높을수록 중요 → 여기서는 테이블 호환성을 위해 (1-score)로 변환 가능 옵션 제공
"""

from __future__ import annotations
from typing import List, Dict
import re


def _normalize_kw(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip().lower()
    return s


# -------- YAKE --------
def extract_yake(text: str, topk: int = 15, ngram_max: int = 3, dedup: float = 0.9) -> List[Dict]:
    import yake
    ke = yake.KeywordExtractor(lan="en", n=ngram_max, top=topk, dedupLim=dedup, features=None)
    out = []
    for kw, score in ke.extract_keywords(text):
        k = _normalize_kw(kw)
        if len(k) < 3 or not re.search(r"[a-z]", k):
            continue
        out.append({"keyword": k[:255], "score": float(score), "method": "yake"})
    return out


# -------- KeyBERT --------
_KBERT = None  # lazy singleton


def extract_keybert(text: str, topk: int = 15, ngram_range=(1, 3), invert_score: bool = True) -> List[Dict]:
    global _KBERT
    if _KBERT is None:
        from keybert import KeyBERT
        # lightweight sentence-transformers 모델(영문 기사 기준)
        _KBERT = KeyBERT(model="all-MiniLM-L6-v2")

    cands = _KBERT.extract_keywords(
        text,
        keyphrase_ngram_range=ngram_range,
        stop_words="english",
        use_maxsum=True,
        nr_candidates=max(40, topk * 2),
        top_n=topk,
    )
    out = []
    for kw, score in cands:
        k = _normalize_kw(kw)
        if len(k) < 3:
            continue
        s = (1.0 - float(score)) if invert_score else float(score)
        out.append({"keyword": k[:255], "score": s, "method": "keybert"})
    return out


# -------- Unified API --------
def extract_keywords(text: str, method: str = "yake", topk: int = 15) -> List[Dict]:
    method = (method or "yake").lower()
    if method == "yake":
        return extract_yake(text, topk=topk)
    elif method == "keybert":
        return extract_keybert(text, topk=topk)
    else:
        raise ValueError(f"unknown method: {method}")
