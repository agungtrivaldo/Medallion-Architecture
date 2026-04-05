from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook


TABLE = {
    "users": {
        "query": """
            SELECT
                user_id,
                email,
                password_hash,
                phone_number,
                created_at,
                updated_at,
                current_timestamp AS _ingested_at,
                "postgres_oltp" AS _source_system
            FROM postgres.public.users
            """,
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
                'postgres_oltp' as _source_system
            FROM postgres.public.orders
        """,
        "watermark": "updated_at",
        "primary_key": "order_id"
    },
    "brands" : {
        "query": """
            SELECT 
                brand_id,
                brand_name,
                country_of_origin,
                current_timestamp AS _ingested_at,
                'postgres_oltp' AS _source_system
            FROM postgres.public.brands
        """,
        "watermark": "",
        "primary_key": "brand_id"
    },
    "categories" : {
        "query": """
            SELECT 
                category_id,
                category_name,
                current_timestamp AS _ingested_at,
                'postgres_oltp' AS _source_system 
            FROM postgres.public.categories
        """,
        "watermark": "",
        "primary_key": "category_id"
    },
    "order_items" : {
        "query": """
        SELECT order_items.item_id,
            order_items.order_id,
            order_items.product_id,
            order_items.quantity,
            CAST(order_items.unit_price_at_purchase as double) as unit_price_at_purchase,
            orders.updated_at,
            current_timestamp AS _ingested_at,
            'postgres_oltp' AS _source_system
        FROM postgres.public.order_items 
        LEFT JOIN postgres.public.orders ON orders.order_id = order_items.order_id
    """,
        "watermark": "updated_at",
        "primary_key": "item_id"
    },
    "payments" : {
        "query": """
            SELECT 
                payments.payment_id,
                payments.order_id,
                payments.payment_method,
                payments.payment_status,
                payments.payment_date,
                orders.updated_at,
                current_timestamp AS _ingested_at,
                'postgres_oltp' AS _source_system
            FROM postgres.public.payments 
            LEFT JOIN postgres.public.orders ON orders.order_id = payments.order_id
            """,
        "watermark": "updated_at",
        "primary_key": "payment_id"
    },
    "products" : {
        "query": """
            SELECT 
                product_id,
                category_id,
                brand_id,
                product_name,
                CAST(base_price as double) AS base_price,
                weight_grams,
                updated_at,
                current_timestamp AS _ingested_at,
                'postgres_oltp' AS _source_system 
            FROM postgres.public.products
        """,
        "watermark": "",
        "primary_key": "product_id"
    },
    "shipping" : {
        "query": """
        SELECT 
            shipping.shipping_id,
            shipping.order_id,
            shipping.courier_name,
            shipping.tracking_number,
            cast(shipping.shipping_cost as double) AS shipping_cost,
            shipping.shipping_status,
            orders.updated_at,
            current_timestamp AS _ingested_at,
            'postgres_oltp' AS _source_system
        FROM postgres.public.shipping
        LEFT JOIN postgres.public.orders ON orders.order_id = shipping.order_id
        """,
        "watermark": "updated_at",
        "primary_key": "shipping_id"
    },
    "user_addresses" : {
        "query": """
            SELECT 
                user_addresses.address_id,
                user_addresses.user_id,
                user_addresses.province,
                user_addresses.city,
                user_addresses.postal_code,
                user_addresses.full_address,
                users.created_at,
                users.updated_at,
                current_timestamp AS _ingested_at,
                'postgres_oltp' AS _source_system 
            FROM postgres.public.user_addresses
            LEFT JOIN postgres.public.users on users.user_id = user_addresses.user_id
            """,
        "watermark": "updated_at",
        "primary_key": "user_id"
    },
}


@dag(
    dag_id="etl_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def etl_pipeline():
    @task.branch
    def table_checking(table):
        trino_hook = TrinoHook(trino_conn_id="trino_conn")
        sql = f"SHOW TABLES FROM iceberg.bronze LIKE '{table}'"
        records = trino_hook.get_records(sql)
        watermark = TABLE[table]["watermark"]
        if not records or watermark == "":
            return f"create_table_{table}"
        else:
            return f"upsert_table_{table}"
        
    @task
    def create_table(config,table):
        trino_hook = TrinoHook(trino_conn_id="trino_conn")
        sql = f"CREATE OR REPLACE TABLE iceberg.bronze.{table} AS {config['query']}"
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
        run_date = context['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
        sql = f"""
                    MERGE INTO iceberg.bronze.{table} bronze
                    USING (
                        {config['query']} WHERE {config['watermark']} >= CAST('{run_date}' AS TIMESTAMP)
                    ) oltp
                    ON bronze.{primary_key} = oltp.{primary_key}
                    WHEN MATCHED THEN UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
                """
        trino_hook.run(sql)

    @task.bash(trigger_rule="none_failed")
    def dbt_ingest_silver():
        cmd = """
            set -e
            cd /opt/airflow/dbt_config
            dbt snapshot 
        """
        return cmd
    
    bronze = []

    for table, config in TABLE.items():
        checker = table_checking.override(task_id=f"table_checking_{table}")(table)
        creator = create_table.override(task_id=f"create_table_{table}")(config,table)
        upsert = upsert_table.override(task_id=f"upsert_table_{table}")(config,table)

        checker >> [creator,upsert]
        bronze.extend([creator,upsert])
        
    silver_ingest = dbt_ingest_silver()

    bronze >> silver_ingest

etl_pipeline()