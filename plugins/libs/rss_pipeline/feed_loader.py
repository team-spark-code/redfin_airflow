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
        return [], state

    resp.raise_for_status()

    # feedparser 로 파싱
    parsed = feedparser.parse(resp.content)

    # 상태 갱신 (ETag/Modified 우선: 응답 헤더 → feedparser 결과)
    state.etag = resp.headers.get("ETag") or getattr(parsed, "etag", None)
    state.modified = resp.headers.get("Last-Modified") or getattr(parsed, "modified", None)
    state.fetched_ts = time.time()
    _save_state(feed_url, state_dir, state)

    # entries는 feedparser가 표준화한 dict 형태
    return list(parsed.entries or []), state


def iter_feeds(feed_urls: Iterable[str], state_dir: str | Path, timeout: float = 15.0) -> Iterable[dict]:
    """
    여러 피드를 순회하며 entry를 하나씩 yield 합니다.
    - 내부적으로 load_feed()를 호출
    - 피드 간 예외는 개별 처리하고 다음 피드로 진행(견고성)
    """
    for url in feed_urls:
        try:
            entries, _ = load_feed(url, state_dir=state_dir, timeout=timeout)
            for e in entries:
                yield {"feed_url": url, "entry": e}
        except Exception as e:
            # 문제 있는 피드는 경고만 남기고 계속 진행하도록 설계
            # (Airflow 태스크 로그에서 확인 가능)
            print(f"[feed_loader] WARN: failed to load {url}: {e}")
            continue
