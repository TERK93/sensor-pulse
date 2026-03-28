with base as (
    select *
    from {{ ref('stg_sensor_readings') }}
    where status = 'valid'
)

select
    engine_id,
    max(cycle)               as total_cycles,
    round(avg(sensor_02), 2) as avg_sensor_02,
    round(avg(sensor_04), 2) as avg_sensor_04,
    round(avg(sensor_11), 2) as avg_sensor_11,
    round(stddev(sensor_02), 3) as stddev_sensor_02,
    case
        when max(cycle) > 200 then 'long_life'
        when max(cycle) > 130 then 'medium_life'
        else 'short_life'
    end as life_category

from base
group by engine_id
order by total_cycles desc