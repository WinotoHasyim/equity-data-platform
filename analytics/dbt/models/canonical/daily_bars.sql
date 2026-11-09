{{
    config(
        materialized='table',
        partition_by={
            'field': 'timestamp',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['symbol', 'feed']
    )
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by bar_key
            order by ingested_at desc, run_id desc
        ) as observation_rank
    from {{ ref('stg_daily_bars') }}
)

select * except (observation_rank)
from ranked
where observation_rank = 1
