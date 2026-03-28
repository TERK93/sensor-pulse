with base as (
    select *
    from {{ ref('stg_sensor_readings') }}
    where status = 'valid'
),

early as (
    select
        dataset_id,
        engine_id,
        avg(sensor_02) as s02_early,
        avg(sensor_04) as s04_early,
        avg(sensor_11) as s11_early
    from base
    where cycle <= 30
    group by dataset_id, engine_id
),

late as (
    select
        dataset_id,
        engine_id,
        avg(sensor_02) as s02_late,
        avg(sensor_04) as s04_late,
        avg(sensor_11) as s11_late
    from base
    where rul <= 30
    group by dataset_id, engine_id
)

select
    e.dataset_id,
    e.engine_id,
    round(l.s02_late - e.s02_early, 3) as s02_drift,
    round(l.s04_late - e.s04_early, 3) as s04_drift,
    round(l.s11_late - e.s11_early, 3) as s11_drift
from early e
join late l on e.dataset_id = l.dataset_id and e.engine_id = l.engine_id
order by abs(s02_drift) desc