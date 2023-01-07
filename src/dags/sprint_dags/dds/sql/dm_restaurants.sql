
truncate  table dds.dm_restaurants cascade;
insert into dds.dm_restaurants 
(restaurant_id,restaurant_name,active_from,active_to)
select 
object_id as restaurant_id,
replace (cast (js::json->'name' as text),'"','' ) as restaurant_name,
active_from,
active_to

from
(select 
	object_id,
	cast (object_value as json) as js,
	update_ts as active_from,
	cast ('2099-12-31 00:00:00.000' as timestamp) as active_to
	
from stg.ordersystem_restaurants) as t