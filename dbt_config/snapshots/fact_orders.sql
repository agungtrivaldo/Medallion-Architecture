{% snapshot fact_orders %}

{{
    config(
        target_schema = 'silver',
        unique_key = 'order_id',
        strategy = 'timestamp',
        update_at = 'updated_at'
    )
}}

SELECT 
    CAST(user_id AS VARCHAR(5)) as user_id , 
    CAST(order_id AS VARCHAR(20)) as order_id, 
    CAST(total_amount AS DOUBLE) as total_amount,
    CAST(address_id AS VARCHAR(255)) as address_id, 
    CAST(order_date AS TIMESTAMP) as order_date, 
    CAST(order_status AS VARCHAR(15)) as order_status, 
    CAST(updated_at AS TIMESTAMP) as updated_at,
    CAST(_ingested_at AS TIMESTAMP) as _ingested_at,
    CAST(_source_system AS VARCHAR(100)) as _source_system

FROM {{source ('bronze_layer','orders')}}

{% endsnapshot %}