# Redfin Airflow 로컬 실행 가이드

이 문서는 Redfin Airflow를 로컬 환경에서 실행하는 방법을 설명합니다.

## 🚀 빠른 시작

### 1단계: 환경 설정
```bash
cd redfin_airflow
chmod +x scripts/*.sh
./scripts/local_setup.sh
```

### 2단계: Airflow 실행
```bash
# 터미널 1에서 웹서버 실행
./scripts/start_local.sh webserver

# 터미널 2에서 스케줄러 실행
./scripts/start_local.sh scheduler
```

### 3단계: 웹 UI 접속
- 브라우저에서 http://localhost:8085 접속
- 사용자: `redfin`
- 비밀번호: `Redfin7620!`

## 📁 프로젝트 구조

```
redfin_airflow/
├── scripts/                    # 실행 스크립트
│   ├── local_setup.sh         # 초기 환경 설정
│   └── start_local.sh         # Airflow 실행
├── dags/                      # DAG 파일들
│   ├── local_test_dag.py      # 로컬 테스트용 DAG
│   └── ...                    # 기타 DAG들
├── plugins/                   # 커스텀 플러그인
├── config/                    # 설정 파일
├── data/                      # 데이터 저장소
├── logs/                      # 로그 파일
├── venv/                      # Python 가상환경
├── env.local                  # 환경 변수
└── requirements.txt           # Python 의존성
```

## 🔧 상세 설정

### 환경 변수 설정
`env.local` 파일에서 환경 변수를 수정할 수 있습니다:

```bash
# 기본 설정
AIRFLOW_HOME=/home/user/workspace/redfin/redfin_airflow
AIRFLOW__WEBSERVER__PORT=8085
AIRFLOW_ENV=dev
DATA_ROOT=/home/user/workspace/redfin/data

# 데이터베이스 설정 (SQLite)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/user/workspace/redfin/redfin_airflow/airflow.db
```

### Python 가상환경
```bash
# 가상환경 생성
python3 -m venv venv

# 가상환경 활성화
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt
```

## 🎯 사용법

### 스크립트 사용법

#### 환경 설정
```bash
./scripts/local_setup.sh
```
- Python 가상환경 생성
- 의존성 설치
- Airflow 데이터베이스 초기화
- 관리자 사용자 생성

#### Airflow 실행
```bash
# 웹서버 시작
./scripts/start_local.sh webserver

# 스케줄러 시작
./scripts/start_local.sh scheduler

# 상태 확인
./scripts/start_local.sh status
```

#### 수동 실행
```bash
# 환경 변수 설정
export AIRFLOW_HOME=/home/user/workspace/redfin/redfin_airflow

# 가상환경 활성화
source venv/bin/activate

# 웹서버 실행
airflow webserver --port 8085

# 스케줄러 실행
airflow scheduler
```

## 🧪 테스트

### 테스트 DAG 실행
`local_test_dag.py`가 자동으로 로드되어 10분마다 실행됩니다:

1. **hello_world**: 간단한 Hello World 메시지
2. **get_airflow_info**: 환경 정보 출력
3. **bash_hello**: Bash 명령어 실행

### 로그 확인
```bash
# Airflow 로그 확인
tail -f logs/scheduler/latest/*.log
tail -f logs/dag_processor_manager/*.log

# DAG 실행 로그
airflow tasks logs local_test_dag hello_world latest
```

## 🔍 문제 해결

### 일반적인 문제들

#### 1. 포트 충돌
```bash
# 포트 사용 확인
netstat -tlnp | grep 8085

# 다른 포트 사용
export AIRFLOW__WEBSERVER__PORT=8086
```

#### 2. 권한 문제
```bash
# 스크립트 실행 권한 부여
chmod +x scripts/*.sh

# 가상환경 권한 확인
ls -la venv/
```

#### 3. 데이터베이스 문제
```bash
# 데이터베이스 재초기화
airflow db reset

# 사용자 재생성
airflow users create \
    --username redfin \
    --firstname red \
    --lastname fin \
    --role Admin \
    --password Redfin7620! \
    --email skybluee0612@gmail.com
```

#### 4. DAG 로딩 문제
```bash
# DAG 파일 권한 확인
ls -la dags/

# Python 문법 검사
python3 -m py_compile dags/local_test_dag.py

# Airflow DAG 목록 확인
airflow dags list
```

### 로그 레벨 조정
```bash
# 상세 로그 활성화
export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG

# 특정 모듈 로그 레벨 조정
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
```

## 📊 모니터링

### Airflow 상태 확인
```bash
# DAG 상태 확인
airflow dags state local_test_dag

# 태스크 상태 확인
airflow tasks state local_test_dag hello_world latest

# 실행 중인 DAG 확인
airflow dags list-running
```

### 시스템 리소스 모니터링
```bash
# 프로세스 확인
ps aux | grep airflow

# 메모리 사용량
ps aux | grep airflow | awk '{sum+=$6} END {print sum/1024 " MB"}'

# 포트 사용 확인
netstat -tlnp | grep 8085
```

## 🚀 프로덕션 고려사항

로컬 환경은 개발 및 테스트용입니다. 프로덕션 환경에서는 다음을 고려하세요:

1. **데이터베이스**: PostgreSQL 또는 MySQL 사용
2. **Executor**: CeleryExecutor 또는 KubernetesExecutor
3. **보안**: HTTPS, 인증, 권한 관리
4. **모니터링**: Prometheus, Grafana 등
5. **백업**: 정기적인 데이터베이스 백업

## 📚 추가 리소스

- [Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Airflow 로컬 설치 가이드](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)
- [Airflow 설정 가이드](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html)
