import logging

from airflow import DAG, utils
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator

args = {
    "owner": "airflow",
    "start_date": utils.dates.days_ago(7),
    "provide_context": True
}

try:
    tmpl_search_path = Variable.get("sql_path")
except KeyError:
    logging.info("Ensure init_setting is backfilled")
    tmpl_search_path = "/usr/local/airflow/sql"

dag = DAG(
    "init_etl_intervention",
    schedule_interval="@once",
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1
)

t1 = PostgresOperator(
    task_id="init_etl_intervention",
    sql="warehouse_intervention_tables.sql",
    postgres_conn_id="warehouse_db",
    dag=dag
)
