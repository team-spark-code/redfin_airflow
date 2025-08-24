# plugins/hooks/mariadb_hook.py
# -*- coding: utf-8 -*-
"""
MariaDB Hook (SQLAlchemy 기반)
- Airflow Connection에서 접속 정보를 읽어 SQLAlchemy Engine 제공
- 간단 실행/컨텍스트 매니저 유틸 포함
- 환경변수 폴백 지원 (Airflow Connection이 없을 때)

필요 패키지:
  - apache-airflow-providers-mysql
  - SQLAlchemy
  - PyMySQL

설정 우선순위:
  1. Airflow Connection (권장)
  2. 환경변수 (폴백)
  3. 기본값 (최후)

환경변수 설정 (폴백용):
  - MARIADB_HOST: 데이터베이스 호스트
  - MARIADB_PORT: 데이터베이스 포트
  - MARIADB_USER: 데이터베이스 사용자
  - MARIADB_PASSWORD: 데이터베이스 비밀번호
  - MARIADB_DATABASE: 데이터베이스 이름

Airflow Connection 예시 (Conn ID: mariadb_default)
- Conn Type: 'mysql'
- Host: mariadb
- Schema: redfin
- Login: redfin
- Password: ****
- Port: 3306
"""
from __future__ import annotations
from typing import Optional, Iterable, Mapping, Any, Union
from contextlib import contextmanager
import os
from pathlib import Path

from airflow.hooks.base import BaseHook
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class MariaDBHook(BaseHook):
    """
    SQLAlchemy 엔진을 생성하고 간단한 실행 헬퍼를 제공.
    - Airflow Connection 우선, 환경변수 폴백
    """

    def __init__(self, mariadb_conn_id: Optional[str] = "mariadb_default") -> None:
        super().__init__()
        self.conn_id = mariadb_conn_id
        self._engine: Optional[Engine] = None

    def _load_env_defaults(self) -> dict:
        """
        환경변수에서 기본 설정을 읽어온다 (폴백용).
        """
        return {
            "host": os.getenv("MARIADB_HOST", "localhost"),
            "port": int(os.getenv("MARIADB_PORT", "3306")),
            "user": os.getenv("MARIADB_USER", "redfin"),
            "password": os.getenv("MARIADB_PASSWORD", ""),  # 보안상 빈 문자열
            "database": os.getenv("MARIADB_DATABASE", "redfin"),
        }

    def _load_connection_info(self) -> dict:
        """
        Airflow Connection 또는 환경변수에서 연결 정보를 읽어온다.
        """
        # 1. Airflow Connection 시도
        if self.conn_id:
            try:
                conn = BaseHook.get_connection(self.conn_id)
                if conn and conn.host:
                    self.log.info("Using Airflow Connection: %s", self.conn_id)
                    return {
                        "host": conn.host,
                        "port": conn.port or 3306,
                        "user": conn.login or "",
                        "password": conn.password or "",
                        "database": conn.schema or "",
                        "source": "airflow_connection"
                    }
            except Exception as e:
                self.log.warning("Failed to load Airflow Connection %s: %s", self.conn_id, e)

        # 2. 환경변수 폴백
        env_config = self._load_env_defaults()
        if env_config["password"]:  # 비밀번호가 설정된 경우만
            self.log.info("Using environment variables for MariaDB connection")
            env_config["source"] = "environment"
            return env_config
        
        # 3. 기본값 (개발/테스트용)
        self.log.warning("Using default connection settings (development only)")
        return {
            "host": "localhost",
            "port": 3306,
            "user": "redfin",
            "password": "",
            "database": "redfin",
            "source": "default"
        }

    def get_engine(self, pool_pre_ping: bool = True, pool_recycle: int = 3600) -> Engine:
        """
        SQLAlchemy Engine 생성.
        - Connection 정보는 Airflow Connection 우선, 환경변수 폴백
        """
        if self._engine is not None:
            return self._engine

        config = self._load_connection_info()
        
        # 연결 정보 검증
        if not config["user"] or not config["database"]:
            raise ValueError("Database user and database name are required")
        
        # URL 생성
        url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        
        self.log.info("Creating SQLAlchemy engine for %s:%s/%s (source: %s)", 
                     config['host'], config['port'], config['database'], config['source'])
        
        try:
            self._engine = create_engine(
                url, 
                pool_pre_ping=pool_pre_ping, 
                pool_recycle=pool_recycle,
                pool_size=10,
                max_overflow=20,
                echo=False  # SQL 로깅 비활성화 (성능 향상)
            )
            return self._engine
        except Exception as e:
            self.log.error("Failed to create SQLAlchemy engine: %s", e)
            raise

    def test_connection(self) -> bool:
        """
        데이터베이스 연결을 테스트한다.
        """
        try:
            with self.get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
                self.log.info("Database connection test successful")
                return True
        except Exception as e:
            self.log.error("Database connection test failed: %s", e)
            return False

    @contextmanager
    def begin(self) -> Engine.connect:
        """
        트랜잭션 컨텍스트 매니저.
        사용:
            with hook.begin() as conn:
                conn.execute(text("..."), {...})
        """
        eng = self.get_engine()
        try:
            with eng.begin() as conn:
                yield conn
        except SQLAlchemyError as e:
            self.log.error("Database transaction failed: %s", e)
            raise

    # ---- 단순 실행 유틸 ----
    def run(self, sql: str, params: Optional[Mapping[str, Any]] = None) -> None:
        """
        DDL/단순 DML 실행.
        """
        try:
            with self.begin() as conn:
                conn.execute(text(sql), params or {})
            self.log.info("SQL executed successfully: %s", sql[:100] + "..." if len(sql) > 100 else sql)
        except SQLAlchemyError as e:
            self.log.error("SQL execution failed: %s", e)
            raise

    def run_many(self, sql: str, seq_of_params: Iterable[Mapping[str, Any]]) -> None:
        """
        동일 SQL에 여러 파라미터를 일괄 바인딩 실행.
        """
        try:
            with self.begin() as conn:
                conn.execute(text(sql), list(seq_of_params))
            self.log.info("Batch SQL executed successfully: %s rows", len(list(seq_of_params)))
        except SQLAlchemyError as e:
            self.log.error("Batch SQL execution failed: %s", e)
            raise

    def execute_query(self, sql: str, params: Optional[Mapping[str, Any]] = None) -> list:
        """
        SELECT 쿼리를 실행하고 결과를 반환한다.
        """
        try:
            with self.get_engine().connect() as conn:
                result = conn.execute(text(sql), params or {})
                rows = result.fetchall()
                self.log.info("Query executed successfully: %s rows returned", len(rows))
                return rows
        except SQLAlchemyError as e:
            self.log.error("Query execution failed: %s", e)
            raise

    def get_connection_info(self) -> dict:
        """
        현재 연결 정보를 반환한다 (디버깅용).
        """
        config = self._load_connection_info()
        return {
            "host": config["host"],
            "port": config["port"],
            "user": config["user"],
            "database": config["database"],
            "source": config["source"]
        }

    def close(self) -> None:
        """
        엔진 연결을 정리한다.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self.log.info("Database engine disposed")
