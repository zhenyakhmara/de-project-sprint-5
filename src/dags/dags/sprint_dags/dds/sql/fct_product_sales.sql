
truncate  table dds.fct_product_sales;
drop TYPE if exists anoop_type_3;
create TYPE anoop_type_3 AS (product_id varchar(100));
drop TYPE if exists anoop_type_4;
create TYPE anoop_type_4 AS (price numeric (14,2) );
drop TYPE if exists anoop_type_5;
create TYPE anoop_type_5 AS (bonus_payment numeric (14,2));
drop TYPE if exists anoop_type_6;
create TYPE anoop_type_6 AS (bonus_grant numeric (14,2) );
drop TYPE if exists anoop_type_7;
create TYPE anoop_type_7 AS (quantity int );

insert into dds.fct_product_sales 
(product_id, order_id, count,price, total_sum, bonus_payment, bonus_grant)
select *
from

(select 
	dm_products.id as product_id,
	dm_orders.id as order_id,
	t3.count,
	t3.price,
	cast (t3.count as numeric (14,2)) * t3.price as total_sum,
	t3.bonus_payment,
	t3.bonus_grant
from
			(select
			order_id,
			(json_populate_recordset (null::anoop_type_3,pp)).product_id as product_id,
			(json_populate_recordset (null::anoop_type_4,pp)).price as price,
			(json_populate_recordset (null::anoop_type_5,pp)).bonus_payment as bonus_payment,
			(json_populate_recordset (null::anoop_type_6,pp)).bonus_grant as bonus_grant,
			(json_populate_recordset (null::anoop_type_7,pp)).quantity as count
			from
				(select
				replace (cast ((ev::json->'order_id')as text),'"', '') as order_id,
				ev::json->'product_payments' as pp
				from
					(select 
						cast (event_value as json) as ev 
					from stg.bonussystem_events
					where event_type = 'bonus_transaction' ) as t) as t2) as t3	
left join dds.dm_orders on t3.order_id = dm_orders.order_key 
left join dds.dm_products on t3.product_id = dm_products.product_id 
) t4	
where t4.order_id is not null	
	