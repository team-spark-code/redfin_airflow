"""
pytest 설정 및 공통 fixture
"""
import os
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Airflow 환경 변수 설정
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT))
os.environ.setdefault("AIRFLOW_ENV", "test")

