truncate table cdm.dm_settlement_report cascade;
insert into cdm.dm_settlement_report
(restaurant_id,restaurant_name ,settlement_date,orders_count ,orders_total_sum ,orders_bonus_payment_sum,orders_bonus_granted_sum, order_processing_fee,restaurant_reward_sum )
select 
	ord.restaurant_id,
	rest.restaurant_name,
	dt.date as settlement_date,
	count (distinct order_id) as orders_count,
	sum (ps.total_sum) as orders_total_sum,
	sum (ps.bonus_payment) as orders_bonus_payment_sum,
	sum (ps.bonus_grant) as orders_bonus_granted_sum,
	sum (ps.total_sum) * 0.25 as  order_processing_fee, -- Сумма комиссии, которую берет компания(25%)
	sum (ps.total_sum) - sum (ps.total_sum)  * 0.25 - sum (ps.bonus_payment) as  restaurant_reward_sum -- Сумма, которую должны заплатить ресторану (общая сумма заказа - сумма комиссии - сумма оплаты бонусами)

from
	dds.fct_product_sales as ps
left join dds.dm_orders as ord on ps.order_id = ord.id
left join dds.dm_restaurants as rest on ord.restaurant_id = rest.id
left join dds.dm_timestamps as dt on ord.timestamp_id = dt.id
where ord.order_status = 'CLOSED'
group by ord.restaurant_id,	rest.restaurant_name,dt.date
on conflict (restaurant_id,settlement_date)
do update SET 
	restaurant_id = EXCLUDED.restaurant_id,
	restaurant_name = EXCLUDED.restaurant_name,
	settlement_date = EXCLUDED.settlement_date,
	orders_count= EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
	orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
	order_processing_fee = EXCLUDED.order_processing_fee,
	restaurant_reward_sum = EXCLUDED.restaurant_reward_sum