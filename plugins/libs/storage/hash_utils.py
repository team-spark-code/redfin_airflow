# libs/storage/hash_utils.py
# -*- coding: utf-8 -*-
"""
해시/유사도 유틸리티.
- URL 고유 식별: MD5(16바이트 바이너리) + hex
- 텍스트 근사중복: 64-bit simhash + Hamming 거리

주의:
- URL 해시는 "정규화된 URL"에 적용하는 것을 권장(utm/gclid 제거 등은 상위 단계에서).
- simhash는 근사 지표이므로 컷오프 기준(예: hamdist<=3)은 데이터 특성에 맞게 조정.
"""

from __future__ import annotations
import hashlib
import re
from typing import Iterable


# ---------- MD5 ----------
def md5_hex(s: str) -> str:
    """MD5 32자리 hex 문자열."""
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def md5_16(s: str) -> bytes:
    """MD5 16바이트 바이너리 (MariaDB BINARY(16) 저장용)."""
    return hashlib.md5(s.encode("utf-8")).digest()


# ---------- 간단 URL 정규화(옵션) ----------
_TRACKING_KEYS = ("utm_", "gclid", "fbclid")


def normalize_url(u: str) -> str:
    """
    매우 경량의 정규화:
      - 앞뒤 공백 제거
      - 스킴/호스트 소문자화
      - 쿼리스트링에서 추적 파라미터 제거(utm_*, gclid, fbclid)
    복잡한 케이스는 url-normalize 같은 전문 라이브러리 사용을 고려.
    """
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
    u = (u or "").strip()
    if not u:
        return u
    p = urlparse(u)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if not any(k.lower().startswith(pref) for pref in _TRACKING_KEYS)]
    p2 = (p.scheme.lower(), p.netloc.lower(), p.path, p.params, urlencode(q), p.fragment)
    return urlunparse(p2)


def url_hash_hex(u: str, normalize: bool = True) -> str:
    """URL → (옵션 정규화) → MD5 hex."""
    return md5_hex(normalize_url(u) if normalize else u)


def url_hash_bin(u: str, normalize: bool = True) -> bytes:
    """URL → (옵션 정규화) → MD5 16바이트."""
    return md5_16(normalize_url(u) if normalize else u)


# ---------- simhash (64-bit) ----------
_WORD_RE = re.compile(r"[A-Za-z0-9_]+", re.UNICODE)


def _tokens(text: str) -> Iterable[str]:
    """
    매우 단순한 토크나이저(영문/숫자/언더스코어).
    필요 시 형태소 분석/stopword 제거로 확장.
    """
    for m in _WORD_RE.finditer(text or ""):
        t = m.group(0).lower()
        if len(t) >= 2:
            yield t


def simhash64(text: str) -> int:
    """
    64-bit simhash. (Charikar 방식)
    - 각 토큰의 MD5를 64비트로 잘라 가중치(+1) 합산 후 부호로 비트 결정
    """
    v = [0] * 64
    for tok in _tokens(text):
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16) & ((1 << 64) - 1)
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(64):
        if v[i] >= 0:
            out |= (1 << i)
    return out


def hamming_distance64(a: int, b: int) -> int:
    """64-bit Hamming distance."""
    x = a ^ b
    # Kernighan’s bit count
    cnt = 0
    while x:
        x &= x - 1
        cnt += 1
    return cnt


def is_near_duplicate(a_text: str, b_text: str, threshold: int = 3) -> bool:
    """
    simhash 기반 근사중복 판정.
    - threshold: 허용 Hamming 거리(작을수록 엄격). 일반 기사 본문은 3~5 제안.
    """
    return hamming_distance64(simhash64(a_text), simhash64(b_text)) <= threshold
