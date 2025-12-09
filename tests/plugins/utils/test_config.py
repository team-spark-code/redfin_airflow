"""
Config 유틸리티 테스트
"""
import os
import pytest
from pathlib import Path
from plugins.utils.config import load_config, data_dir_for


def test_load_config():
    """설정 로드 테스트"""
    config = load_config()
    
    assert "project" in config
    assert "timezone" in config
    assert "data_root" in config
    assert config["project"] == "redfin"


def test_data_dir_for():
    """데이터 디렉토리 경로 생성 테스트"""
    dag_id = "test_dag"
    ts_iso = "2025-08-24T05-00-00Z"
    
    path = data_dir_for(dag_id, ts_iso)
    
    assert isinstance(path, Path)
    assert dag_id in str(path)
    assert ts_iso in str(path)

