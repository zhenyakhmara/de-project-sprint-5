
truncate table dds.dm_orders cascade;
insert into dds.dm_orders 
(order_key, order_status, restaurant_id,timestamp_id, user_id )
	select  
	t2.order_key,
	t2.order_status,
	dm_restaurants.id as restaurant_id,
	dm_timestamps.id  as timestamp_id,
	dm_users.id as user_id
	
		from
			(select
			replace (cast ((js::json->'_id')as text),'"', '') as order_key,
			replace (cast ((js::json->'final_status')as text),'"', '') as order_status,
			replace (cast ((js::json->'restaurant'->'id')as text),'"', '') as restaurant_id,
			cast (replace (cast ((js::json->'date')as text),'"', '')as timestamp) as order_date,
			replace (cast ((js::json->'user'->'id')as text),'"', '') as user_id
			from
				(select 
					cast (object_value as json) as js 
				from stg.ordersystem_orders) as t) t2	
	left join dds.dm_restaurants on t2.restaurant_id = dm_restaurants.restaurant_id 
	left join dds.dm_timestamps on t2.order_date = dm_timestamps.ts
	left join dds.dm_users on t2.user_id = dm_users.user_id
	
	
	;