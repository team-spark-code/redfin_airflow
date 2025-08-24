from airflow import DAG
from airflow.utils.dates import days_ago

DEFAULTS = dict(
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner":"airflow","retries":1},
)

def build_dag(dag_id: str, schedule: str, tags=None, **kwargs):
    return DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        tags=tags or [],
        **{**DEFAULTS, **kwargs}
    )
