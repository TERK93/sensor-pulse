with base as (
    select *
    from {{ ref('stg_sensor_readings') }}
    where status = 'valid'
)

select
    engine_id,
    cycle,
    cycle_pct,
    rul,
    sensor_02,
    sensor_04,
    sensor_11,

    -- Rolling mean (10 cycles)
    avg(sensor_02) over (
        partition by engine_id order by cycle
        rows between 9 preceding and current row
    ) as s02_mean_10,

    -- Rolling stddev (instability indicator)
    stddev(sensor_02) over (
        partition by engine_id order by cycle
        rows between 9 preceding and current row
    ) as s02_std_10,

    -- Rate of change
    sensor_02 - lag(sensor_02, 1) over (
        partition by engine_id order by cycle
    ) as s02_delta,

    sensor_04 - lag(sensor_04, 1) over (
        partition by engine_id order by cycle
    ) as s04_delta

from base
order by engine_id, cycle