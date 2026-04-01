from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook


TABLE = {
    "users": {
        "query": "SELECT * FROM postgres.public.users",
        "watermark": "updated_at",
        "primary_key": "user_id"
    },
    "orders": {
        "query": """
            SELECT 
                user_id, 
                order_id, 
                CAST(total_amount AS DOUBLE) as total_amount,
                address_id, 
                order_date, 
                order_status, 
                updated_at,
                current_timestamp as _ingested_at,
                "postgres_oltp" as _source_system
            FROM postgres.public.orders
        """,
        "watermark": "updated_at",
        "primary_key": "order_id"
    }
}


@dag(
    dag_id="oltp_to_bronze",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def oltp_to_bronze():
    @task.branch
    def table_checking(table):
        trino_hook = TrinoHook(trino_conn_id="trino_conn")
        sql = f"SHOW TABLES FROM iceberg.bronze LIKE '{table}'"
        records = trino_hook.get_records(sql)
        if not records:
            return f"create_table_{table}"
        else:
            return f"upsert_table_{table}"
    @task
    def create_table(config,table):
        trino_hook = TrinoHook(trino_conn_id="trino_conn")
        sql = f"CREATE TABLE iceberg.bronze.{table} AS {config['query']}"
        trino_hook.run(sql)
    @task
    def upsert_table(config,table,**context):
        trino_hook = TrinoHook(trino_conn_id="trino_conn")
        get_cols = f"SELECT column_name FROM postgres.information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'"
        records = trino_hook.get_records(get_cols)
        cols = [row[0] for row in records]
        primary_key =  config["primary_key"]
        update_set = ", ".join([f"{col} = oltp.{col}" for col in cols if col != primary_key])
        insert_cols = ", ".join(cols)
        insert_values = ", ".join([f"oltp.{col}" for col in cols])
        run_date = context['data_interval_start'].isoformat()
        sql = f"""
                    MERGE INTO iceberg.bronze.{table} bronze
                    USING (
                        {config['query']} WHERE {config['watermark']} >= CAST('{run_date}' AS TIMESTAMP)
                    ) oltp
                    ON {primary_key} = oltp.{primary_key}
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
                """
        trino_hook.run(sql)

    for table, config in TABLE.items():
        checker = table_checking.override(task_id=f"table_checking_{table}")(table)
        creator = create_table.override(task_id=f"create_table_{table}")(config,table)
        upsert = upsert_table.override(task_id=f"upsert_table_{table}")(config,table)

        checker >> [creator,upsert]

oltp_to_bronze()