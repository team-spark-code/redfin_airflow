"""
hit_rss 스크래핑 DAG
RSS 피드 수집 및 본문 추출을 위한 Airflow 워크플로우
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
import os
import json
from pathlib import Path

# DAG 기본 설정
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
    'hit_rss_scraping',
    default_args=default_args,
    description='RSS 피드 수집 및 본문 추출 DAG',
    schedule_interval='0 */6 * * *',  # 6시간마다 실행
    catchup=False,
    tags=['scraping', 'rss', 'news'],
)

# 환경 변수 설정
SCRAPY_PROJECT_DIR = Variable.get("SCRAPY_PROJECT_DIR", "/opt/airflow/redfin_scraper")
DATA_OUTPUT_DIR = Variable.get("DATA_OUTPUT_DIR", "/opt/airflow/data/out")
FEEDS_CONFIG_PATH = Variable.get("FEEDS_CONFIG_PATH", "/opt/airflow/feeds/feeds.yaml")

def setup_environment(**context):
    """환경 설정 및 디렉토리 생성"""
    import os
    from pathlib import Path
    
    # 디렉토리 생성
    Path(DATA_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(FEEDS_CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    # 환경 변수 설정
    os.environ['SCRAPY_PROJECT_DIR'] = SCRAPY_PROJECT_DIR
    os.environ['DATA_OUTPUT_DIR'] = DATA_OUTPUT_DIR
    os.environ['FEEDS_CONFIG_PATH'] = FEEDS_CONFIG_PATH
    
    print(f"환경 설정 완료: {SCRAPY_PROJECT_DIR}")
    return "환경 설정 완료"

def collect_rss_feeds(**context):
    """RSS 피드 수집"""
    import subprocess
    import json
    from datetime import datetime
    
    output_file = f"{DATA_OUTPUT_DIR}/rss_feed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    cmd = [
        "scrapy", "crawl", "rss_feed",
        "-o", output_file,
        "-s", "FEEDS=" + json.dumps({output_file: {"format": "jsonlines"}})
    ]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRAPY_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"RSS 피드 수집 완료: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"RSS 피드 수집 실패: {e.stderr}")
        raise

def extract_articles(**context):
    """RSS 피드에서 본문 추출"""
    import subprocess
    import json
    from datetime import datetime
    
    # 이전 태스크의 결과 가져오기
    ti = context['task_instance']
    rss_output = ti.xcom_pull(task_ids='collect_rss_feeds')
    
    if not rss_output:
        raise ValueError("RSS 피드 파일 경로를 찾을 수 없습니다")
    
    output_file = f"{DATA_OUTPUT_DIR}/articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    cmd = [
        "scrapy", "crawl", "article_extractor",
        "-a", f"input={rss_output}",
        "-o", output_file,
        "-s", "FEEDS=" + json.dumps({output_file: {"format": "jsonlines"}})
    ]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRAPY_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"본문 추출 완료: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"본문 추출 실패: {e.stderr}")
        raise

def collect_naver_news(**context):
    """네이버 뉴스 수집 (선택사항)"""
    import subprocess
    import json
    from datetime import datetime
    from pathlib import Path
    
    # naver_ifrs17.json 파일 확인
    naver_data_file = Path(SCRAPY_PROJECT_DIR).parent / "naver_scraper" / "naver_scraper" / "naver_ifrs17.json"
    
    if not naver_data_file.exists():
        print("네이버 뉴스 데이터 파일이 없어 건너뜁니다")
        return None
    
    output_file = f"{DATA_OUTPUT_DIR}/naver_articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    cmd = [
        "scrapy", "crawl", "naver_news",
        "-o", output_file,
        "-s", "FEEDS=" + json.dumps({output_file: {"format": "jsonlines"}})
    ]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRAPY_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"네이버 뉴스 수집 완료: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"네이버 뉴스 수집 실패: {e.stderr}")
        return None

def integrate_data(**context):
    """데이터 통합 및 요약 생성"""
    import json
    from datetime import datetime
    from pathlib import Path
    
    ti = context['task_instance']
    articles_output = ti.xcom_pull(task_ids='extract_articles')
    naver_output = ti.xcom_pull(task_ids='collect_naver_news')
    
    if not articles_output:
        print("본문 추출 파일이 없어 통합을 건너뜁니다")
        return
    
    # 통합 파일 생성
    all_articles_output = f"{DATA_OUTPUT_DIR}/all_articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    with open(all_articles_output, 'w', encoding='utf-8') as outfile:
        # RSS 뉴스 추가
        if Path(articles_output).exists():
            with open(articles_output, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
        
        # 네이버 뉴스 추가
        if naver_output and Path(naver_output).exists():
            with open(naver_output, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
    
    # 통계 계산
    rss_count = sum(1 for _ in open(articles_output, 'r', encoding='utf-8')) if Path(articles_output).exists() else 0
    naver_count = sum(1 for _ in open(naver_output, 'r', encoding='utf-8')) if naver_output and Path(naver_output).exists() else 0
    total_count = sum(1 for _ in open(all_articles_output, 'r', encoding='utf-8'))
    
    # 실행 요약 생성
    summary_file = f"{DATA_OUTPUT_DIR}/execution_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    summary_info = {
        "execution_timestamp": datetime.now().isoformat(),
        "execution_type": "airflow_dag_completion",
        "total_entries": total_count,
        "rss_news_count": rss_count,
        "naver_news_count": naver_count,
        "output_files": {
            "rss_feed": articles_output,
            "naver_news": naver_output,
            "integrated": all_articles_output
        }
    }
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(summary_info, ensure_ascii=False) + '\n')
    
    print(f"데이터 통합 완료: {all_articles_output}")
    print(f"총 뉴스 수: {total_count}개 (RSS: {rss_count}, 네이버: {naver_count})")

# 태스크 정의
setup_env = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag,
)

collect_rss = PythonOperator(
    task_id='collect_rss_feeds',
    python_callable=collect_rss_feeds,
    dag=dag,
)

extract_articles_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_articles,
    dag=dag,
)

collect_naver = PythonOperator(
    task_id='collect_naver_news',
    python_callable=collect_naver_news,
    dag=dag,
)

integrate_data_task = PythonOperator(
    task_id='integrate_data',
    python_callable=integrate_data,
    dag=dag,
)

# 태스크 의존성 설정
setup_env >> collect_rss >> extract_articles_task >> [collect_naver, integrate_data_task]
collect_naver >> integrate_data_task
