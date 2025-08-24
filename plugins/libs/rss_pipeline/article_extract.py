# plugins/rss_pipeline/feed_loader.py
# -*- coding: utf-8 -*-
"""
RSS/Atom 피드 로더.
- feedparser + ETag/Last-Modified 기반 조건부 요청 지원
- 피드별 상태를 state_dir 에 JSON으로 캐시(간단하고 투명한 방식)

주요 함수:
- load_feed(feed_url, state_dir): (entries, state) 반환
- iter_feeds(feed_urls, state_dir): 여러 피드를 순회하며 entries 스트리밍 반환

참고:
- 네트워크 실패는 호출 측에서 재시도 정책을 정하세요(Airflow 재시도 권장).
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Any, Iterable, List, Tuple
from pathlib import Path
import json
import hashlib
import time

import requests
import feedparser


@dataclass
class FeedState:
    etag: str | None = None
    modified: str | None = None  # HTTP Last-Modified 헤더
    fetched_ts: float | None = None  # epoch seconds(진단용)

    def asdict(self) -> Dict[str, Any]:
        return asdict(self)


def _state_path_for(feed_url: str, state_dir: Path) -> Path:
    h = hashlib.md5(feed_url.encode("utf-8")).hexdigest()
    return state_dir / f"feed_{h}.json"


def _load_state(feed_url: str, state_dir: Path) -> FeedState:
    p = _state_path_for(feed_url, state_dir)
    if not p.exists():
        return FeedState()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return FeedState(**data)
    except Exception:
        return FeedState()


def _save_state(feed_url: str, state_dir: Path, state: FeedState) -> None:
    p = _state_path_for(feed_url, state_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state.asdict(), ensure_ascii=False, indent=2), encoding="utf-8")


def load_feed(feed_url: str, state_dir: str | Path, timeout: float = 15.0) -> Tuple[List[dict], FeedState]:
    """
    단일 피드를 로드하고 entries 리스트와 최신 상태를 반환합니다.
    - 서버가 304(Not Modified)를 응답하면 entries는 빈 리스트가 됩니다.
    """
    state_dir = Path(state_dir)
    state = _load_state(feed_url, state_dir)

    headers: Dict[str, str] = {}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.modified:
        headers["If-Modified-Since"] = state.modified

    resp = requests.get(feed_url, headers=headers, timeout=timeout)
    if resp.status_code == 304:
        # 변경 없음
        state.fetched_ts = time.time()
        _save_state(feed_url, state_dir, state)
