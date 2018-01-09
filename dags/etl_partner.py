"""
Prototype: Denomalize partners_partnerorganization table into warehouse
"""
import datetime
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def denormalize_partner_data(**kwargs):
    pg_source = PostgresHook(postgres_conn_id="etools_db")
    pg_target = PostgresHook(postgres_conn_id="warehouse_db")

    get_schemas = """select distinct schemaname from pg_tables"""
    schemas = pg_source.get_records(get_schemas)

    schema_exists = """SELECT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = 'partners_partnerorganization'
    )"""
    for schema_row in schemas:
        schema_name = schema_row[0]

        exists = pg_source.get_first(schema_exists.format(schema=schema_name))
        if not exists[0]:
            continue

        clear_partners = "DELETE FROM partner_organization WHERE schema_name = '{schema}'".format(
            schema=schema_name
        )
        pg_target.run(clear_partners)

        get_partners = """SELECT
            '{schema}',
            a.id,
            a.partner_type,
            a.name,
            a.short_name,
            a.description,
            a.shared_with,
            a.shared_partner,
            a.street_address,
            a.city,
            a.postal_code,
            a.country,
            a.address,
            a.email,
            a.phone_number,
            a.vendor_number,
            a.alternate_id,
            a.alternate_name,
            a.rating,
            a.type_of_assessment,
            a.last_assessment_date,
            a.core_values_assessment_date,
            a.core_values_assessment,
            a.cso_type,
            a.vision_synced,
            a.blocked,
            a.hidden,
            a.deleted_flag,
            a.total_ct_cp,
            a.total_ct_cy,
            a.hact_values
        FROM {schema}.partners_partnerorganization AS a
        """.format(schema=schema_name)

        partners_insert = """INSERT INTO partner_organization (
        "schema_name",
        "partner_organization_id",
        "partner_type",
        "name",
        "short_name",
        "description",
        "shared_with",
        "shared_partner",
        "street_address",
        "city",
        "postal_code",
        "country",
        "address",
        "email",
        "phone_number",
        "vendor_number",
        "alternate_id",
        "alternate_name",
        "rating",
        "type_of_assessment",
        "last_assessment_date",
        "core_values_assessment_date",
        "core_values_assessment",
        "cso_type",
        "vision_synced",
        "blocked",
        "hidden",
        "deleted_flag",
        "total_ct_cp",
        "total_ct_cy",
        "hact_values"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        for row in pg_source.get_records(get_partners):
            row = list(row)
            row[30] = json.dumps(row[30])
            for i in range(0, len(row)):
                if isinstance(row[i], (datetime.datetime, datetime.date)):
                    row[i] = str(row[i])
            pg_target.run(partners_insert, parameters=row)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.utcnow(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_partner",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=1),
    schedule_interval="@daily"
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=denormalize_partner_data,
    dag=dag,
)
