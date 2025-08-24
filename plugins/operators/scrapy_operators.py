# plugins/operators/scrapy_operators.py
# -*- coding: utf-8 -*-
"""
Scrapy를 Airflow 태스크에서 실행하여 JSONL 산출물을 생성하는 Operator.
- Scrapy 프로젝트 디렉터리로 이동 후 spider 실행
- FEEDS 설정을 통해 outfile(JSON Lines)로 저장
- 실패 시 명확한 예외 발생
"""
from __future__ import annotations
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional
import os
import shlex
import subprocess


class ScrapyExportJsonlOperator(BaseOperator):
    """
    Parameters
    ----------
    scrapy_project : str
        Scrapy 프로젝트 루트 디렉터리 경로
    spider : str
        실행할 스파이더 이름
    outfile : str
        산출 JSONL 경로 (Jinja 템플릿 가능: 예 `.../{{ ts }}/articles.jsonl`)
    per_request_delay : float
        DOWNLOAD_DELAY 설정(요청 사이 지연, 초)
    concurrent_requests : int
        CONCURRENT_REQUESTS 설정(동시 요청 수)
    """

    @apply_defaults
    def __init__(
        self,
        scrapy_project: str,
        spider: str,
        outfile: str,
        per_request_delay: float = 0.5,
        concurrent_requests: int = 8,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.scrapy_project = scrapy_project
        self.spider = spider
        self.outfile = outfile
        self.per_request_delay = per_request_delay
        self.concurrent_requests = concurrent_requests

    def _render(self, s: str, context) -> str:
        """Airflow의 템플릿 엔진으로 문자열 렌더링."""
        return self.render_template(s, context)

    def execute(self, context):
        # 템플릿 렌더
        project = self._render(self.scrapy_project, context)
        outfile = self._render(self.outfile, context)

        # 출력 디렉터리 생성
        outdir = os.path.dirname(outfile)
        os.makedirs(outdir, exist_ok=True)

        # Scrapy 실행 커맨드 구성
        # -s FEEDS='{ "path": {"format":"jsonlines"} }'
        feeds_json = (
            '{'
            f'"{outfile}": {{"format": "jsonlines"}}'
            '}'
        )

        cmd = (
            f"cd {shlex.quote(project)} && "
            f"scrapy crawl {shlex.quote(self.spider)} "
            f"-s LOG_LEVEL=INFO "
            f"-s DOWNLOAD_DELAY={self.per_request_delay} "
            f"-s CONCURRENT_REQUESTS={self.concurrent_requests} "
            f"-s FEEDS='{feeds_json}'"
        )

        self.log.info("Running command: %s", cmd)

        # subprocess로 실행(에러 발생 시 예외)
        proc = subprocess.run(
            cmd, shell=True, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        self.log.info("Scrapy output:\n%s", proc.stdout)

        if proc.returncode != 0:
            raise RuntimeError(f"Scrapy process failed (rc={proc.returncode}). See logs above.")

        # downstream 태스크에서 사용할 경로를 XCom으로 반환
        return outfile
