{{ config(
    materialized = 'view'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_live_history') }}

),

deduped as (

    select *,
        row_number() over (
            partition by room_id
            order by ingested_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        room_id,

        nullif(live_title_raw, 'N/A')                                  as live_title,

        cast(
            regexp_extract(duration_raw, r'^(\d{4}-\d{2}-\d{2})')
            as date
        )                                                              as live_date,

        cast(
            regexp_extract(duration_raw, r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
            as timestamp
        )                                                              as start_time,

        cast(
            regexp_extract(duration_raw, r'- (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
            as timestamp
        )                                                              as end_time,

        cast(
            regexp_extract(duration_raw, r'\((\d+) minutes?\)')
            as int64
        )                                                              as duration_mins,

        cast(regexp_replace(nullif(total_view_raw,   'N/A'), r',', '') as int64) as total_views,
        cast(regexp_replace(nullif(total_gifter_raw, 'N/A'), r',', '') as int64) as total_gifters,
        cast(regexp_replace(nullif(total_likes_raw,  'N/A'), r',', '') as int64) as total_likes,

        ingested_at

    from deduped
    where row_num = 1

)

select * from cleaned