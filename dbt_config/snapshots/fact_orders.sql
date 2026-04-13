{% snapshot fact_orders %}

{{
    config(
        target_schema = 'silver',
        unique_key = 'order_id',
        strategy = 'timestamp',
        updated_at = 'updated_at'
    )
}}

SELECT 
    CAST(user_id AS VARCHAR(5)) AS user_id, 
    CAST(order_id AS VARCHAR(20)) AS order_id, 
    CAST(COALESCE(total_amount, 0) AS DOUBLE) AS total_amount,
    CAST(address_id AS VARCHAR(255)) AS address_id, 
    CAST(order_date AS TIMESTAMP) AS order_date, 
    CAST(
        UPPER(SUBSTR(TRIM(COALESCE(order_status, 'Unknown')), 1, 1)) || 
        LOWER(SUBSTR(TRIM(COALESCE(order_status, 'Unknown')), 2)) 
    AS VARCHAR(15)) AS order_status, 
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
    CAST(_source_system AS VARCHAR(100)) AS _source_system

FROM {{source ('bronze_layer','orders')}}

{% endsnapshot %}