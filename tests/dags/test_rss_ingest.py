"""
RSS ingest DAG 테스트
"""
import pytest
from airflow.models import DagBag


@pytest.fixture
def dag_bag():
    """DAG Bag 인스턴스 생성"""
    return DagBag(dag_folder="dags", include_examples=False)


def test_rss_ingest_dag_loaded(dag_bag):
    """RSS ingest DAG가 정상적으로 로드되는지 확인"""
    dag = dag_bag.get_dag(dag_id="rss_ingest")
    assert dag is not None
    assert len(dag.tasks) > 0


def test_rss_ingest_dag_structure(dag_bag):
    """RSS ingest DAG 구조 확인"""
    dag = dag_bag.get_dag(dag_id="rss_ingest")
    
    # 필수 태스크 확인
    task_ids = [task.task_id for task in dag.tasks]
    assert "start" in task_ids
    assert "crawl_rss" in task_ids
    assert "rss_to_docs" in task_ids
    assert "end" in task_ids


def test_rss_ingest_dag_no_import_errors(dag_bag):
    """DAG import 오류 확인"""
    assert len(dag_bag.import_errors) == 0, "DAG import 오류가 있습니다"


def test_rss_ingest_dag_tags(dag_bag):
    """DAG 태그 확인"""
    dag = dag_bag.get_dag(dag_id="rss_ingest")
    assert "rss" in dag.tags
    assert "ingest" in dag.tags

