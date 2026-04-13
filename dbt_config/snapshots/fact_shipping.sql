{% snapshot fact_shipping %}

{{
    config(
        target_schema = 'silver',
        unique_key = 'shipping_id',
        strategy = 'timestamp',
        updated_at = 'updated_at'
    )
}}

SELECT 
	CAST(shipping_id as varchar(5)) as shipping_id,
	CAST(order_id as varchar(20)) as order_id,
	CAST(
        UPPER(SUBSTR(TRIM(COALESCE(courier_name, 'Unknown')), 1, 1)) || 
        LOWER(SUBSTR(TRIM(COALESCE(courier_name, 'Unknown')), 2)) 
    AS VARCHAR(15)) AS courier_name, 
	cast(tracking_number as VARCHAR(15)) as tracking_number,
    cast(shipping_cost as double ) as shipping_cost,
	CAST(
        UPPER(SUBSTR(TRIM(COALESCE(shipping_status, 'Unknown')), 1, 1)) || 
        LOWER(SUBSTR(TRIM(COALESCE(shipping_status, 'Unknown')), 2)) 
    AS VARCHAR(15)) AS shipping_status, 
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
    CAST(_source_system AS VARCHAR(100)) AS _source_system

FROM {{source ('bronze_layer','shipping')}}

{% endsnapshot %}