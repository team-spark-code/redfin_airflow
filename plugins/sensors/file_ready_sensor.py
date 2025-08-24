# plugins/sensors/file_ready_sensor.py
# -*- coding: utf-8 -*-
"""
파일 준비 여부를 확인하는 커스텀 Sensor.
- 존재/크기/라인 수/최종 수정시각 등 간단한 조건을 조합할 수 있음.
- Airflow의 poke_interval/timeout 과 함께 사용.

예:
FileReadySensor(
    task_id="wait_jsonl",
    filepath="/opt/airflow/data/rss_ingest/{{ ts }}/articles.jsonl",
    min_size_bytes=1024,     # 최소 1KB
    min_lines=10,            # 최소 10라인
    mtime_within_sec=3600,   # 최근 1시간 내 갱신
    poke_interval=10,
    timeout=600,
)
"""

from __future__ import annotations
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional
from pathlib import Path
import time


class FileReadySensor(BaseSensorOperator):
    """
    파일 존재 및 간단한 건강성 조건을 확인하는 Sensor.

    Parameters
    ----------
    filepath : str
        검사할 파일 경로(템플릿 가능)
    min_size_bytes : int | None
        파일 최소 크기(Bytes). None이면 비검사
    min_lines : int | None
        파일 최소 라인 수(JSONL 등). None이면 비검사
    mtime_within_sec : int | None
        최근 수정 시간이 현재 시각 기준 이 값 이내여야 함. None이면 비검사
    """

    template_fields = ("filepath",)

    @apply_defaults
    def __init__(
        self,
        filepath: str,
        min_size_bytes: Optional[int] = None,
        min_lines: Optional[int] = None,
        mtime_within_sec: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.min_size_bytes = min_size_bytes
        self.min_lines = min_lines
        self.mtime_within_sec = mtime_within_sec

    def poke(self, context) -> bool:
        # 템플릿 렌더링
        path = Path(self.render_template(self.filepath, context))

        if not path.exists():
            self.log.info("File not found yet: %s", path)
            return False

        # 크기 확인
        if self.min_size_bytes is not None:
            size = path.stat().st_size
            if size < self.min_size_bytes:
                self.log.info("File exists but too small (%d < %d): %s", size, self.min_size_bytes, path)
                return False

        # 라인 수 확인(JSONL/텍스트 파일 전용)
        if self.min_lines is not None:
            try:
                with path.open("r", encoding="utf-8", errors="ignore") as f:
                    lines = 0
                    for _ in f:
                        lines += 1
                        if lines >= self.min_lines:
                            break
                if lines < self.min_lines:
                    self.log.info("File exists but insufficient lines (%d < %d): %s", lines, self.min_lines, path)
                    return False
            except Exception as e:
                self.log.info("Failed to count lines: %s", e)
                return False

        # 최종 수정시각 확인
        if self.mtime_within_sec is not None:
            mtime = path.stat().st_mtime
            age = time.time() - mtime
            if age > self.mtime_within_sec:
                self.log.info("File is stale (age=%.1fs > %ds): %s", age, self.mtime_within_sec, path)
                return False

        # 모든 조건 통과
        self.log.info("File is ready: %s", path)
        return True
