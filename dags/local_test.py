"""
로컬 테스트를 위한 간단한 DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# 기본 인수 설정
default_args = {
    'owner': 'redfin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'local_test_dag',
    default_args=default_args,
    description='로컬 테스트용 DAG',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=['local', 'test'],
)

def hello_world():
    """간단한 Hello World 함수"""
    logging.info("Hello from Redfin Airflow!")
    return "Hello World!"

def get_airflow_info():
    """Airflow 환경 정보 출력"""
    import os
    info = {
        'AIRFLOW_HOME': os.getenv('AIRFLOW_HOME', 'Not set'),
        'AIRFLOW_ENV': os.getenv('AIRFLOW_ENV', 'Not set'),
        'DATA_ROOT': os.getenv('DATA_ROOT', 'Not set'),
        'Python Version': os.sys.version,
        'Current Time': datetime.now().isoformat(),
    }
    
    for key, value in info.items():
        logging.info(f"{key}: {value}")
    
    return info

# 태스크 정의
hello_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag,
)

info_task = PythonOperator(
    task_id='get_airflow_info',
    python_callable=get_airflow_info,
    dag=dag,
)

bash_task = BashOperator(
    task_id='bash_hello',
    bash_command='echo "Hello from Bash!" && date',
    dag=dag,
)

# 태스크 의존성 설정
hello_task >> info_task >> bash_task
