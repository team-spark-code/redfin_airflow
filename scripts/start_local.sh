#!/bin/bash

# Redfin Airflow 로컬 실행 스크립트
set -euo pipefail

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# 스크립트 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 함수: 환경 변수 설정
setup_environment() {
    log_info "환경 변수를 설정합니다..."
    export AIRFLOW_HOME="$PROJECT_DIR"
    export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
    export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_DIR/plugins"
    export AIRFLOW__CORE__EXECUTOR=LocalExecutor
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$PROJECT_DIR/airflow.db"
    export AIRFLOW__CORE__LOAD_EXAMPLES=false
    export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
    export AIRFLOW__WEBSERVER__PORT=8085
    export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    export AIRFLOW_ENV=dev
    export DATA_ROOT="$PROJECT_DIR/data"
    
    # .env.local 파일이 있으면 로드
    if [ -f "$PROJECT_DIR/env.local" ]; then
        log_info "env.local 파일을 로드합니다..."
        set -a
        source "$PROJECT_DIR/env.local"
        set +a
    fi
    
    # 환경 변수 우선순위 설정 (스크립트 설정이 env.local보다 우선)
    export AIRFLOW_HOME="$PROJECT_DIR"
    export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
    export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_DIR/plugins"
    export AIRFLOW__CORE__EXECUTOR=LocalExecutor
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$PROJECT_DIR/airflow.db"
    export AIRFLOW__CORE__LOAD_EXAMPLES=false
    export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
    export AIRFLOW__WEBSERVER__PORT=8085
    export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    export AIRFLOW_ENV=dev
    export DATA_ROOT="$PROJECT_DIR/data"
}

# 함수: 가상환경 활성화
activate_venv() {
    if [ ! -d "$PROJECT_DIR/venv" ]; then
        log_error "가상환경이 존재하지 않습니다. 먼저 local_setup.sh를 실행하세요."
        exit 1
    fi
    
    log_info "가상환경을 활성화합니다..."
    source "$PROJECT_DIR/venv/bin/activate"
}

# 함수: Airflow 상태 확인
check_airflow_status() {
    log_info "Airflow 상태를 확인합니다..."
    
    # 데이터베이스 파일 확인
    if [ ! -f "$PROJECT_DIR/airflow.db" ]; then
        log_error "Airflow 데이터베이스가 초기화되지 않았습니다."
        log_info "다음 명령어를 실행하세요:"
        echo "  cd $PROJECT_DIR && ./scripts/local_setup.sh"
        exit 1
    fi
    
    # DAG 파일 확인
    if [ ! -d "$PROJECT_DIR/dags" ] || [ -z "$(ls -A "$PROJECT_DIR/dags" 2>/dev/null)" ]; then
        log_warn "DAG 파일이 없습니다."
    fi
}

# 함수: 웹서버 시작
start_webserver() {
    log_header "Airflow 웹서버를 시작합니다"
    log_info "포트: 8085"
    log_info "브라우저에서 http://localhost:8085 접속"
    log_info "사용자: redfin, 비밀번호: Redfin7620!"
    echo
    
    airflow webserver --port 8085
}

# 함수: 스케줄러 시작
start_scheduler() {
    log_header "Airflow 스케줄러를 시작합니다"
    echo
    
    airflow scheduler
}

# 메인 실행 로직
main() {
    log_header "Redfin Airflow 로컬 실행"
    
    # 환경 변수 설정
    setup_environment
    
    # 가상환경 활성화
    activate_venv
    
    # Airflow 상태 확인
    check_airflow_status
    
    # 명령어 인수에 따른 실행
    case "${1:-}" in
        "webserver"|"web")
            start_webserver
            ;;
        "scheduler"|"sched")
            start_scheduler
            ;;
        "status")
            log_header "Airflow 상태 정보"
            echo "AIRFLOW_HOME: $AIRFLOW_HOME"
            echo "DAGS_FOLDER: $AIRFLOW__CORE__DAGS_FOLDER"
            echo "PLUGINS_FOLDER: $AIRFLOW__CORE__PLUGINS_FOLDER"
            echo "EXECUTOR: $AIRFLOW__CORE__EXECUTOR"
            echo "DATABASE: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
            echo "PORT: $AIRFLOW__WEBSERVER__PORT"
            echo "ENV: $AIRFLOW_ENV"
            echo "DATA_ROOT: $DATA_ROOT"
            ;;
        *)
            log_info "사용법: $0 {webserver|scheduler|status}"
            echo
            echo "  webserver  - 웹서버 시작 (포트 8085)"
            echo "  scheduler  - 스케줄러 시작"
            echo "  status     - 현재 설정 상태 확인"
            echo
            echo "예시:"
            echo "  # 터미널 1에서 웹서버 실행:"
            echo "  $0 webserver"
            echo
            echo "  # 터미널 2에서 스케줄러 실행:"
            echo "  $0 scheduler"
            ;;
    esac
}

# 스크립트 실행
main "$@"
