"""Initialize connections and variables"""
import logging

from airflow import DAG, models, utils
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session


args = {
    "owner": "airflow",
    "start_date": utils.dates.days_ago(7),
    "provide_context": True
}


def init_settings():
    logging.info("Creating connections, pools, and variables")
    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = "postgres"
        new_conn.host = attributes.get("host")
        new_conn.port = "5432"
        new_conn.schema = attributes.get("schema")
        new_conn.login = "postgres"

        session.add(new_conn)
        session.commit()

    create_new_conn(session, {
        "conn_id": "etools_db",
        "host": "db",
        "schema": "etools",
    })

    create_new_conn(session, {
        "conn_id": "warehouse_db",
        "host": "warehouse",
        "schema": "warehouse",
    })

    new_var = models.Variable()
    new_var.key = "sql_path"
    new_var.set_val("/usr/local/airflow/sql")
    session.add(new_var)
    session.commit()

    new_pool = models.Pool()
    new_pool.pool = "warehouse_db"
    new_pool.slots = 10
    new_pool.description = "Allows max. 10 connections to the Warehouse"
    session.add(new_pool)
    session.commit()

    session.close()


dag = DAG(
    "init_settings",
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1
)

t1 = PythonOperator(
    task_id="init_settings",
    python_callable=init_settings,
    provide_context=False,
    dag=dag
)
