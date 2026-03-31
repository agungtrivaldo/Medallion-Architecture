from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook

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
        "primary_key":"order_id",
        "columns":["user_id,order_id,CAST(total_amount AS DOUBLE) as total_amount,address_id,order_date,order_status,updated_at"]
    }
    
}

@dag(
    dag_id="oltp_to_bronze",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def oltp_to_bronze():
    def incremental(config,table_name,**context):
        trino_hook =  Trinohook(trino_conn_id="trino_conn")
        query = f"SELECT column_name FROM postgres.information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}'"
        records = trino_hook.get_records(query)
        cols = [[row[0]] for row in records]
        primary_key =  config["primary_key"]
        run_date = context['data_interval_start'].isformat()
        update_set = ", ".join([f"{cols} = oltp.{cols}" for cols in cols if cols != primary_key])
        insert_cols = ", ".join(cols)
        insert_values = ", ".join([f"oltp.{cols}" for cols in cols])
        sql = f"""
                    MERGE INTO iceberg.bronze.orders bronze
                    USING (
                        {config['query']} WHERE {config['watermark']} >= '{{{{ data_interval_start }}}}'
                    ) oltp
                    ON {primary_key} = oltp.{primary_key}
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT {insert_cols} VALUES {insert_values}
                """


    for table, config in TABLE.items():
        if config["type"] == "full":
            SQLExecuteQueryOperator(
                task_id = f"full_load_{table}",
                conn_id = "trino_conn",
                sql = f"""
                    DROP TABLE IF EXIST iceberg.bronze.{table};
                    CREATE TABLE icberg.bronze.{table} AS {config['query']};
                """
        )
        else:
            incremental(config=config ,table_name=table)


oltp_to_bronze()