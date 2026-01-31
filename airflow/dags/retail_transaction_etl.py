from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

DAG_ID = "oltp_to_dwh_retail_transactions"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["etl", "oltp", "dwh"],
) as dag:

    @task
    def extract_from_oltp():
        oltp = PostgresHook(postgres_conn_id="oltp_postgres")

        last_sync = Variable.get(
            "retail_transactions_last_sync",
            default_var="1970-01-01 00:00:00"
        )

        sql = """
        SELECT
            id,
            customer_id,
            last_status,
            pos_origin,
            pos_destination,
            created_at,
            updated_at,
            deleted_at
        FROM retail_transactions
        WHERE updated_at > %s
        """

        records = oltp.get_records(sql, parameters=(last_sync,))
        return records

    @task
    def load_to_dwh(records):
        if not records:
            return "No data to load"

        dwh = PostgresHook(postgres_conn_id="dwh_postgres")
        conn = dwh.get_conn()
        cur = conn.cursor()

        upsert_sql = """
        INSERT INTO dwh_retail_transactions (
            id,
            customer_id,
            last_status,
            pos_origin,
            pos_destination,
            created_at,
            updated_at,
            deleted_at
        ) VALUES %s
        ON CONFLICT (id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            last_status = EXCLUDED.last_status,
            pos_origin = EXCLUDED.pos_origin,
            pos_destination = EXCLUDED.pos_destination,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at
        """

        execute_values(cur, upsert_sql, records)
        conn.commit()

        cur.close()
        conn.close()

        max_updated_at = max(r[6] for r in records)
        Variable.set("retail_transactions_last_sync", str(max_updated_at))

        return f"Upserted {len(records)} rows"
    records = extract_from_oltp()
    load_to_dwh(records)
