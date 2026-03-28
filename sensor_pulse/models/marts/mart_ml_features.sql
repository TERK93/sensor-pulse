with base as (
    select *
    from {{ ref('stg_sensor_readings') }}
    where status = 'valid'
)

select
    dataset_id,
    engine_id,
    cycle,
    cycle_pct,
    -- Cap RUL at 125 — standard CMAPSS practice, reduces noise in early-life cycles
    least(rul, 125) as rul,
    op_setting_1,
    op_setting_2,
    sensor_02,
    sensor_04,
    sensor_11,

    -- Rolling mean (10 cycles)
    avg(sensor_02) over (
        partition by dataset_id, engine_id order by cycle
        rows between 9 preceding and current row
    ) as s02_mean_10,

    -- Rolling stddev (instability indicator)
    stddev(sensor_02) over (
        partition by dataset_id, engine_id order by cycle
        rows between 9 preceding and current row
    ) as s02_std_10,

    -- Rate of change
    sensor_02 - lag(sensor_02, 1) over (
        partition by dataset_id, engine_id order by cycle
    ) as s02_delta,

    sensor_04 - lag(sensor_04, 1) over (
        partition by dataset_id, engine_id order by cycle
    ) as s04_delta

from base
order by dataset_id, engine_id, cycle