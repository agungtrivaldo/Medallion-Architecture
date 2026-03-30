from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

TABLE = {
    "users":{
        "type":"full",
        "query":"SELECT * FROM postgres.public.users"
    },
    "orders": {
        "type":"incremental",
        "query":"""
            SELECT 
                user_id,
                order_id,
                CAST(total_amount AS DOUBLE) as total_amount,
                address_id,
                order_date,
                order_status,
                updated_at
            FROM postgres.public.orders
            """,
        "watermark":"updated_at",
        "primary_key":"order_id"
    }
    
}

@dag(
    dag_id="oltp_to_bronze",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def oltp_to_bronze():
    for table, config in TABLE.items():
        if config["type"] == "full":
            ingest_table = SQLExecuteQueryOperator(
                task_id = f"full_load_{table}",
                conn_id = "trino_conn",
                sql = f"""
                    DROP TABLE IF EXIST iceberg.bronze.{table};
                    CREATE TABLE icberg.bronze.{table} AS {config['query']};
                """
        )
        else:
            ingest_table = SQLExecuteQueryOperator(
                task_id = f"incremental_load_{table}",
                conn_id = "trino_conn",
                sql = f"""
                    MERGE INTO iceberg.bronze.orders bronze
                    USING (
                        {config['query']} WHERE {config['watermark']} >= '{{{{ data_interval_start }}}}'
                    ) oltp
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT VALUES (*)
                """
            )

        ingest_table  

oltp_to_bronze()