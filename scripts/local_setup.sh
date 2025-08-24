#!/bin/bash

# Redfin Airflow 로컬 환경 설정 스크립트
set -euo pipefail

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 스크립트 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

log_info "Redfin Airflow 로컬 환경 설정을 시작합니다..."
log_info "프로젝트 디렉토리: $PROJECT_DIR"

# Python 가상환경 확인 및 생성
if [ ! -d "$PROJECT_DIR/venv" ]; then
    log_info "Python 가상환경을 생성합니다..."
    python3 -m venv "$PROJECT_DIR/venv"
fi

# 가상환경 활성화
log_info "가상환경을 활성화합니다..."
source "$PROJECT_DIR/venv/bin/activate"

# 의존성 설치
log_info "Python 의존성을 설치합니다..."
pip install --upgrade pip
pip install -r "$PROJECT_DIR/requirements.txt"

# Airflow 환경 변수 설정
log_info "Airflow 환경 변수를 설정합니다..."
export AIRFLOW_HOME="$PROJECT_DIR"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_DIR/plugins"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$PROJECT_DIR/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
export AIRFLOW__WEBSERVER__PORT=8085
export AIRFLOW__CORE__FERNET_KEY="$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")"
export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
export AIRFLOW_ENV=dev
export DATA_ROOT="$PROJECT_DIR/data"

# 필요한 디렉토리 생성
log_info "필요한 디렉토리를 생성합니다..."
mkdir -p "$PROJECT_DIR/data"
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/config"

# Airflow 데이터베이스 초기화
log_info "Airflow 데이터베이스를 초기화합니다..."
airflow db init

# 관리자 사용자 생성
log_info "관리자 사용자를 생성합니다..."
airflow users create \
    --username redfin \
    --firstname red \
    --lastname fin \
    --role Admin \
    --password Redfin7620! \
    --email skybluee0612@gmail.com

log_info "로컬 환경 설정이 완료되었습니다!"
log_info "다음 명령어로 Airflow를 실행할 수 있습니다:"
echo
echo "  # 터미널 1에서 웹서버 실행:"
echo "  source $PROJECT_DIR/venv/bin/activate"
echo "  export AIRFLOW_HOME=$PROJECT_DIR"
echo "  airflow webserver --port 8085"
echo
echo "  # 터미널 2에서 스케줄러 실행:"
echo "  source $PROJECT_DIR/venv/bin/activate"
echo "  export AIRFLOW_HOME=$PROJECT_DIR"
echo "  airflow scheduler"
echo
echo "  # 브라우저에서 http://localhost:8085 접속"
echo "  # 사용자: redfin, 비밀번호: Redfin7620!"
