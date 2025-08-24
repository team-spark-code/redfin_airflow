# plugins/utils/config.py
# -*- coding: utf-8 -*-
"""
환경설정 로더 (YAML + 환경변수 오버라이드)
- 기본값: include/configs/base.yaml
- 환경별 오버레이: include/configs/{AIRFLOW_ENV}.yaml  (예: dev.yaml, prod.yaml)
- ENV 오버라이드: 대문자 키만 일부 지원(필요 시 확장)
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import os
import yaml


_DEFAULT_BASE = Path(f"{os.getenv('AIRFLOW_HOME')}/include/configs/base.yaml")
_DEFAULT_DIR = Path(f"{os.getenv('AIRFLOW_HOME')}/include/configs")


def _deep_update(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """dict 재귀 병합. src의 값을 dst에 덮어씀."""
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError(f"YAML must be a mapping: {path}")
        return data


def load_config() -> Dict[str, Any]:
    """
    설정 로딩 우선순위:
      1) base.yaml
      2) {AIRFLOW_ENV}.yaml
      3) 환경변수(일부 키만)
    """
    env = os.getenv("AIRFLOW_ENV", "dev")
    base = _read_yaml(_DEFAULT_BASE)
    overlay = _read_yaml(_DEFAULT_DIR / f"{env}.yaml")

    cfg = {}
    _deep_update(cfg, base)
    _deep_update(cfg, overlay)

    # 간단 ENV 오버라이드(필요 시 확장)
    env_overrides = {
        "timezone": os.getenv("TIMEZONE"),
        "data_root": os.getenv("DATA_ROOT"),
    }
    for k, v in env_overrides.items():
        if v:
            cfg[k] = v

    # 필수 기본값 보강
    cfg.setdefault("project", "redfin")
    cfg.setdefault("timezone", "Asia/Seoul")
    cfg.setdefault("data_root", "/opt/airflow/data")
    cfg.setdefault("rss", {}).setdefault("per_request_delay", 0.5)
    cfg["rss"].setdefault("concurrent_requests", 8)
    cfg.setdefault("vectorstore", {}).setdefault("collection", "ai_news_en")
    cfg["vectorstore"].setdefault("embedder", "openai")
    cfg["vectorstore"].setdefault("sentence_model", "all-MiniLM-L6-v2")
    return cfg


def data_dir_for(dag_id: str, ts_iso: str, cfg: Dict[str, Any] | None = None) -> Path:
    """
    산출물 표준 경로 규칙:
      {data_root}/{dag_id}/{ts}/
    - ts_iso: Airflow Jinja '{{ ts }}' 렌더 결과(예: '2025-08-24T01:00:00+00:00')
    """
    cfg = cfg or load_config()
    root = Path(cfg["data_root"])
    return root / dag_id / ts_iso


def ensure_dir(p: Path) -> None:
    """디렉터리 생성(이미 있으면 무시)."""
    p.mkdir(parents=True, exist_ok=True)
