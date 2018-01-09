"""
Prototype: Denomalize partners_agreement table into warehouse
"""
import datetime
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def denormalize_agreement_data(**kwargs):
    pg_source = PostgresHook(postgres_conn_id="etools_db")
    pg_target = PostgresHook(postgres_conn_id="warehouse_db")

    get_schemas = """select distinct schemaname from pg_tables"""
    schemas = pg_source.get_records(get_schemas)

    schema_exists = """SELECT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = 'partners_agreement'
    )"""
    for schema_row in schemas:
        schema_name = schema_row[0]

        exists = pg_source.get_first(schema_exists.format(schema=schema_name))
        if not exists[0]:
            continue

        clear_agreements = "DELETE FROM agreement WHERE schema_name = '{schema}'".format(
            schema=schema_name
        )
        pg_target.run(clear_agreements)

        get_agreements = """SELECT
            '{schema}',
            a.id,
            a.created,
            a.modified,
            a.start,
            a.end,
            a.agreement_type,
            a.agreement_number,
            a.attached_agreement,
            a.signed_by_unicef_date,
            a.signed_by_partner_date,
            a.partner_id,
            a.partner_manager_id,
            a.signed_by_id,
            a.status,
            a.country_programme_id,
            p.partner_type AS partner_partner_type,
            p.name AS partner_name,
            u.first_name AS signed_by_first_name,
            u.last_name AS signed_by_last_name,
            u.email AS signed_by_email,
            pm.title AS partner_manager_title,
            pm.first_name AS partner_manager_first_name,
            pm.last_name AS partner_manager_last_name,
            pm.email AS partner_manager_email,
            pm.phone AS partner_manager_phone
        FROM {schema}.partners_agreement AS a
        LEFT JOIN {schema}.partners_partnerorganization AS p ON a.partner_id = p.id
        LEFT JOIN auth_user AS u ON a.signed_by_id = u.id
        LEFT JOIN {schema}.partners_partnerstaffmember AS pm ON a.partner_manager_id = pm.id
        """.format(schema=schema_name)

        agreements_insert = """INSERT INTO agreement (
        "schema_name",
        "agreement_id",
        "created",
        "modified",
        "start",
        "end",
        "agreement_type",
        "agreement_number",
        "attached_agreement",
        "signed_by_unicef_date",
        "signed_by_partner_date",
        "partner_id",
        "partner_manager_id",
        "signed_by_id",
        "status",
        "country_programme_id",
        "partner_partner_type",
        "partner_name",
        "signed_by_first_name",
        "signed_by_last_name",
        "signed_by_email",
        "partner_manager_title",
        "partner_manager_first_name",
        "partner_manager_last_name",
        "partner_manager_email",
        "partner_manager_phone"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        for row in pg_source.get_records(get_agreements):
            row = list(row)
            for i in range(0, len(row)):
                if isinstance(row[i], (datetime.datetime, datetime.date)):
                    row[i] = str(row[i])
            pg_target.run(agreements_insert, parameters=row)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.utcnow(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_agreement",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=1),
    schedule_interval="@daily"
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=denormalize_agreement_data,
    dag=dag,
)
