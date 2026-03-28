with source as (
    select *
    from read_csv(
        'C:\Users\trott\Documents\Projects\sensor-pulse\data\raw\train_FD001.txt',
        delim=' ',
        header=false,
        null_padding=true,
        ignore_errors=true,
        auto_detect=false,
        columns={
            'engine_id': 'INTEGER',
            'cycle': 'INTEGER',
            'op_setting_1': 'DOUBLE',
            'op_setting_2': 'DOUBLE',
            'op_setting_3': 'DOUBLE',
            'sensor_01': 'DOUBLE', 'sensor_02': 'DOUBLE', 'sensor_03': 'DOUBLE',
            'sensor_04': 'DOUBLE', 'sensor_05': 'DOUBLE', 'sensor_06': 'DOUBLE',
            'sensor_07': 'DOUBLE', 'sensor_08': 'DOUBLE', 'sensor_09': 'DOUBLE',
            'sensor_10': 'DOUBLE', 'sensor_11': 'DOUBLE', 'sensor_12': 'DOUBLE',
            'sensor_13': 'DOUBLE', 'sensor_14': 'DOUBLE', 'sensor_15': 'DOUBLE',
            'sensor_16': 'DOUBLE', 'sensor_17': 'DOUBLE', 'sensor_18': 'DOUBLE',
            'sensor_19': 'DOUBLE', 'sensor_20': 'DOUBLE', 'sensor_21': 'DOUBLE'
        }
    )
),

validated as (
    select
        *,
        case
            when engine_id is null or cycle is null  then 'invalid_null'
            when sensor_02 <= 0 or sensor_04 <= 0   then 'invalid_sensor_range'
            else 'valid'
        end as status,
        max(cycle) over (partition by engine_id) as max_cycle
    from source
)

select
    *,
    round(cycle::double / max_cycle, 3) as cycle_pct,
    (max_cycle - cycle) as rul
from validated