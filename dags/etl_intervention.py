"""
Prototype: Denomalize partners_intervention table into warehouse
"""
import datetime
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def denormalize_intervention_data(**kwargs):
    pg_source = PostgresHook(postgres_conn_id="etools_db")
    pg_target = PostgresHook(postgres_conn_id="warehouse_db")

    get_schemas = """select distinct schemaname from pg_tables"""
    schemas = pg_source.get_records(get_schemas)

    schema_exists = """SELECT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = 'partners_intervention'
    )"""
    for schema_row in schemas:
        schema_name = schema_row[0]

        exists = pg_source.get_first(schema_exists.format(schema=schema_name))
        if not exists[0]:
            continue

        clear_interventions = "DELETE FROM intervention WHERE schema_name = '{schema}'".format(
            schema=schema_name
        )
        pg_target.run(clear_interventions)

        get_interventions = """SELECT
            '{schema}',
            i.*,
            u.first_name AS unicef_signatory_first_name,
            u.last_name AS unicef_signatory_last_name,
            u.email AS unicef_signatory_email,
            p.title AS partner_signatory_title,
            p.first_name AS partner_signatory_first_name,
            p.last_name AS partner_signatory_last_name,
            p.email AS partner_signatory_email,
            p.phone AS partner_signatory_phone,
            ufp.first_name AS unicef_focal_point_first_name,
            ufp.last_name AS unicef_focal_point_last_name,
            ufp.email AS unicef_focal_point_email,
            pfp.title AS partner_focal_point_title,
            pfp.first_name AS partner_focal_point_first_name,
            pfp.last_name AS partner_focal_point_last_name,
            pfp.email AS partner_focal_point_email,
            pfp.phone AS partner_focal_point_phone
        FROM {schema}.partners_intervention AS i
        LEFT JOIN auth_user AS u ON i.unicef_signatory_id = u.id
        LEFT JOIN {schema}.partners_partnerstaffmember AS p ON i.partner_authorized_officer_signatory_id = p.id
        LEFT JOIN {schema}.partners_intervention_unicef_focal_points AS ufp_j ON i.id = ufp_j.intervention_id
        LEFT JOIN auth_user AS ufp ON ufp_j.user_id = ufp.id
        LEFT JOIN {schema}.partners_intervention_partner_focal_points AS pfp_j ON i.id = pfp_j.intervention_id
        LEFT JOIN {schema}.partners_partnerstaffmember AS pfp ON pfp_j.partnerstaffmember_id = pfp.id
        """.format(schema=schema_name)

        interventions_insert = """INSERT INTO intervention (
        "schema_name",
        "intervention_id",
        "created",
        "modified",
        "document_type",
        "number",
        "title",
        "status",
        "start",
        "end",
        "submission_date",
        "submission_date_prc",
        "review_date_prc",
        "prc_review_document",
        "signed_by_unicef_date",
        "signed_by_partner_date",
        "population_focus",
        "agreement_id",
        "partner_authorized_officer_signatory_id",
        "unicef_signatory_id",
        "signed_pd_document",
        "country_programme_id",
        "contingency_pd",
        "metadata",
        "unicef_signatory_first_name",
        "unicef_signatory_last_name",
        "unicef_signatory_email",
        "partner_signatory_title",
        "partner_signatory_first_name",
        "partner_signatory_last_name",
        "partner_signatory_email",
        "partner_signatory_phone",
        "unicef_focal_point_first_name",
        "unicef_focal_point_last_name",
        "unicef_focal_point_email",
        "partner_focal_point_title",
        "partner_focal_point_first_name",
        "partner_focal_point_last_name",
        "partner_focal_point_email",
        "partner_focal_point_phone"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s)"""
        for row in pg_source.get_records(get_interventions):
            row = list(row)
            row[23] = json.dumps(row[23])
            for i in range(0, len(row)):
                if isinstance(row[i], (datetime.datetime, datetime.date)):
                    row[i] = str(row[i])
            pg_target.run(interventions_insert, parameters=row)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.utcnow(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_intervention",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=1),
    schedule_interval="@daily"
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=denormalize_intervention_data,
    dag=dag,
)
