
insert into dds.dm_timestamps 
(ts,year,month,day, time, date )
select 
cast ( tss as timestamp) as ts,
cast (substring (tss, 1,4 ) as int) as year,
extract ( month from cast ( tss as timestamp)) as month,
extract ( day from cast ( tss as timestamp)) as day,
cast ( tss as time) as time,
cast ( tss as date) as time
from
	(select 
	replace (cast (js::json->'date' as text),'"','' ) as tss
	from
		(select 
			cast (object_value as json) as js	
		from stg.ordersystem_orders) as t) t2
ON CONFLICT ON CONSTRAINT dm_timestamps_ts_unique do update set year = excluded.year;

