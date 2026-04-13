{% snapshot fact_orders %}

{{
    config(
        target_schema = 'silver',
        unique_key = 'order_id',
        strategy = 'timestamp',
        updated_at = 'updated_at'
    )
}}

select 
	cast(item_id as varchar(5)) as item_id,
	cast(order_id as varchar(15)) as order_id,
	cast(product_id as varchar (5)) as product_id,
	cast(quantity as int) as quantity,
	cast(unit_price_at_purchase as double) as unit_price_at_purchase,
	CAST(updated_at AS TIMESTAMP) AS updated_at,
  	CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
  	CAST(_source_system AS VARCHAR(100)) AS _source_system

FROM {{source ('bronze_layer','orders')}}

{% endsnapshot %}