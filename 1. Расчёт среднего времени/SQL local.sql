with ut AS
(with unated_table as
(-- Временные таблицы
with
c as
(with step_3 as
(with step_2 as
(select entity_id, created_by,
to_timestamp(created_at) as created_at,
extract(day from to_timestamp(created_at)) as created_day,
CAST(to_timestamp(created_at) AS TIME(0)) as created_time,
ROW_NUMBER() OVER (PARTITION BY entity_id, created_by ORDER BY entity_id, created_at) AS Rnk
from chat_messages
where entity_id IN
(select entity_id -- id с сообщениями и от к. и от м.
from chat_messages
group by entity_id
having count(distinct created_by) > 1)
order by entity_id, created_at)
select entity_id, created_by, created_at, created_day, created_time
from step_2
where rnk = 1)
select entity_id, created_day as day_client, created_time as time_client
from step_3
where created_by = 0),
m as
(with m2 as
(with m1 as
(with step_2 as
(select entity_id, created_by,
to_timestamp(created_at) as created_at,
extract(day from to_timestamp(created_at)) as created_day,
CAST(to_timestamp(created_at) AS TIME(0)) as created_time,
ROW_NUMBER() OVER (PARTITION BY entity_id, created_by ORDER BY entity_id, created_at) AS Rnk
from chat_messages
where entity_id IN
(select entity_id -- id с сообщениями и от к. и от м.
from chat_messages
group by entity_id
having count(distinct created_by) > 1)
order by entity_id, created_at)
select entity_id, created_by, created_at, created_day, created_time
from step_2
where rnk = 1)
select *,
ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY entity_id) AS Rnk_m
from m1
where created_by <> 0)
select entity_id, created_by as manager, created_day as day_manager, created_time as time_manager
from m2
where Rnk_m = 1)
-- Объединение
select
c.entity_id,
c.day_client,
c.time_client,
m.manager,
m.day_manager,
m.time_manager,
(case
WHEN day_client = day_manager and time_client between CAST(to_timestamp('09:30:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('23:59:59','HH24:MI:SS') AS TIME(0)) and time_manager between CAST(to_timestamp('09:30:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('23:59:59','HH24:MI:SS') AS TIME(0)) THEN time_manager - time_client
when day_client = day_manager and time_client between CAST(to_timestamp('00:00:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0)) and time_manager between CAST(to_timestamp('00:00:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0)) then time_manager - time_client
when day_client = day_manager and time_client between CAST(to_timestamp('00:00:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0)) and time_manager between CAST(to_timestamp('09:30:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('23:59:59','HH24:MI:SS') AS TIME(0)) then time_manager - CAST(to_timestamp('09:30:00','HH24:MI:SS') AS TIME(0))
when day_client <> day_manager and time_manager between CAST(to_timestamp('00:00:00','HH24:MI:SS') AS TIME(0)) and CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0)) then CAST(to_timestamp('23:59:59','HH24:MI:SS') AS TIME(0)) - time_client
when day_client <> day_manager and time_manager > CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0)) then time_manager - CAST(to_timestamp('09:29:59','HH24:MI:SS') AS TIME(0))
end) as response_time
from c
join m on c.entity_id = m.entity_id)
select
unated_table.entity_id,
unated_table.day_client,
unated_table.time_client,
unated_table.manager,
unated_table.day_manager,
unated_table.time_manager,
unated_table.response_time,
cast (managers.rop_id as int)
from unated_table
join
managers on unated_table.manager = managers.mop_id
where response_time > CAST(to_timestamp('00:00:00','HH24:MI:SS') AS TIME(0)))
--select *
--from ut
select manager, DATE_TRUNC('second', avg(response_time)) as avg_response_time
from ut
group by manager
order by avg(response_time)