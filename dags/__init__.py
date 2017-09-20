import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule
import os
import site
_mydir = os.path.dirname(__file__)
site.addsitedir(os.path.join(_mydir, '..'))
import do

dag = DAG(
    dag_id='snapshot',
    schedule_interval=datetime.timedelta(minutes=1),
    catchup=False,
    # start_date=datetime.datetime.today() - datetime.timedelta(minutes=5),
    start_date=airflow.utils.dates.days_ago(1),
    end_date=airflow.utils.dates.days_ago(-1)
)

# latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

snapshot = PythonOperator(
        python_callable=do.take_snapshot_write_file,
        task_id='snapshot',
        dag=dag)
# snapshot.set_upstream(latest_only)
