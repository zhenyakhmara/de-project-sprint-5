truncate table dds.dm_products cascade;
drop TYPE if exists anoop_type;
create TYPE anoop_type AS (id varchar(100), name text);
drop TYPE if exists anoop_type_2;
create TYPE anoop_type_2 AS (price numeric (14,2) );
insert into dds.dm_products 
(restaurant_id,product_id,product_name,product_price, active_from, active_to )
	select distinct 
	dm_restaurants.id as restaurant_id,
	(json_populate_recordset (null::anoop_type,order_items)).id as product_id,
	(json_populate_recordset (null::anoop_type,order_items)).name as product_name,
	(json_populate_recordset (null::anoop_type_2,order_items)).price as product_price,
	CAST ('2021-12-31 00:00:00.000' AS TIMESTAMP) as active_from,
	CAST ('2099-12-31 00:00:00.000' AS TIMESTAMP) as active_to
		from
			(select
			replace (cast ((js::json->'restaurant'->'id')as text),'"', '') as restaurant_id,
			js::json->'order_items' as order_items
			--update_ts
			from
				(select 
					cast (object_value as json) as js
					--update_ts 
				from stg.ordersystem_orders) as t) t2	
	left join dds.dm_restaurants on t2.restaurant_id = dm_restaurants.restaurant_id ;