# libs/storage/artifact_path.py
# -*- coding: utf-8 -*-
"""
아티팩트(산출물) 경로 규칙 유틸리티.

표준 규칙
--------
<root>/<dag_id>/<ts_iso>/
  ├─ articles.jsonl        # RSS 수집 원본(HTML 포함)
  ├─ articles.docs.jsonl   # 전처리·청킹 결과
  └─ reports/
      ├─ daily/...
      ├─ weekly/...
      └─ monthly/...

- ts_iso: Airflow Jinja '{{ ts }}' 같은 ISO 문자열 (예: '2025-08-24T01:00:00+00:00')
- root  : config(data_root) 또는 기본 '/opt/airflow/data'
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


DEFAULT_ROOT = "/opt/airflow/data"


@dataclass(frozen=True)
class ArtifactPath:
    """
    아티팩트 경로 생성기.
    """
    dag_id: str
    ts_iso: str
    root: str = DEFAULT_ROOT

    @property
    def base(self) -> Path:
        """<root>/<dag_id>/<ts_iso>/"""
        return Path(self.root) / self.dag_id / self.ts_iso

    # ---------- 공통 산출물 ----------
    @property
    def articles_jsonl(self) -> Path:
        return self.base / "articles.jsonl"

    @property
    def docs_jsonl(self) -> Path:
        return self.base / "articles.docs.jsonl"

    # ---------- 리포트 ----------
    def report_dir(self, period: str) -> Path:
        """
        period: 'daily' | 'weekly' | 'monthly'
        """
        return self.base / "reports" / period

    def daily_report(self, name: str) -> Path:
        """예: name='2025-08-24_summary.csv'"""
        return self.report_dir("daily") / name

    def weekly_report(self, name: str) -> Path:
        return self.report_dir("weekly") / name

    def monthly_report(self, name: str) -> Path:
        return self.report_dir("monthly") / name

    # ---------- 벡터 스토어(Chroma 등) ----------
    def chroma_dir(self, chroma_root: Optional[str] = None) -> Path:
        """
        컬렉션 퍼시스턴스 루트는 config(vectorstore.chroma_dir)로 외부 주입.
        """
        root = chroma_root or "/opt/airflow/chroma"
        return Path(root)

    # ---------- 유틸 ----------
    def ensure_all_dirs(self) -> None:
        """
        base 및 하위 주요 디렉터리 생성.
        """
        for p in [
            self.base,
            self.report_dir("daily"),
            self.report_dir("weekly"),
            self.report_dir("monthly"),
        ]:
            p.mkdir(parents=True, exist_ok=True)


def make_paths(dag_id: str, ts_iso: str, cfg: Optional[Dict] = None) -> Dict[str, str]:
    """
    dict 형태로 경로를 한번에 제공(Operator 템플릿에서 사용하기 쉽도록).

    Returns:
      {
        "dir": "<.../>",
        "articles_jsonl": ".../articles.jsonl",
        "docs_jsonl": ".../articles.docs.jsonl",
        "reports_daily_dir": ".../reports/daily",
        "reports_weekly_dir": ".../reports/weekly",
        "reports_monthly_dir": ".../reports/monthly",
        "chroma_dir": "<vectorstore.chroma_dir or /opt/airflow/chroma>"
      }
    """
    root = (cfg or {}).get("data_root", DEFAULT_ROOT)
    chroma_root = (cfg or {}).get("vectorstore", {}).get("chroma_dir", "/opt/airflow/chroma")

    ap = ArtifactPath(dag_id=dag_id, ts_iso=ts_iso, root=root)
    return {
        "dir": str(ap.base),
        "articles_jsonl": str(ap.articles_jsonl),
        "docs_jsonl": str(ap.docs_jsonl),
        "reports_daily_dir": str(ap.report_dir("daily")),
        "reports_weekly_dir": str(ap.report_dir("weekly")),
        "reports_monthly_dir": str(ap.report_dir("monthly")),
        "chroma_dir": str(ap.chroma_dir(chroma_root)),
    }
