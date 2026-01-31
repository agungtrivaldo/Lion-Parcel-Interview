from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_transactions_hourly_etl",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["oltp", "dwh", "incremental", "soft-delete"]
) as dag:

    upsert_to_dwh = PostgresOperator(
        task_id="upsert_retail_transactions",
        postgres_conn_id="dwh_postgres",
        sql="""
        INSERT INTO dwh_retail_transactions (
            id,
            customer_id,
            last_status,
            pos_origin,
            pos_destination,
            created_at,
            updated_at,
            deleted_at
        )
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
        WHERE updated_at >= (
            SELECT COALESCE(MAX(updated_at), '1970-01-01')
            FROM dwh_retail_transactions
        )
        ON CONFLICT (id)
        DO UPDATE SET
            customer_id     = EXCLUDED.customer_id,
            last_status     = EXCLUDED.last_status,
            pos_origin      = EXCLUDED.pos_origin,
            pos_destination = EXCLUDED.pos_destination,
            updated_at      = EXCLUDED.updated_at,
            deleted_at      = EXCLUDED.deleted_at;
        """
    )

    upsert_to_dwh
