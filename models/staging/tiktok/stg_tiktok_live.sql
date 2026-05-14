{{ config(
    materialized = 'incremental',
    unique_key   = 'room_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_live_history') }}

    {% if is_incremental() %}
    -- Only grab rows that are newer than what we've already processed.
    -- This is the core incremental filter — without it, dbt reads the full table every run.
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

-- Deduplication: if the same room_id was ingested twice (e.g. file re-uploaded),
-- keep only the most recently ingested copy.
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

        -- Strip "N/A" → NULL. All "is it N/A?" logic lives here, not in Python.
        nullif(live_title_raw, 'N/A')                                  as live_title,

        -- Parse the duration string: "2025-01-01 10:00:00 - 2025-01-01 11:30:00 (90 minutes)"
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

        -- Cast metrics; REGEXP_REPLACE strips commas ("1,234" → 1234)
        cast(regexp_replace(nullif(total_view_raw,   'N/A'), r',', '') as int64) as total_views,
        cast(regexp_replace(nullif(total_gifter_raw, 'N/A'), r',', '') as int64) as total_gifters,
        cast(regexp_replace(nullif(total_likes_raw,  'N/A'), r',', '') as int64) as total_likes,

        ingested_at

    from deduped
    where row_num = 1

)

select * from cleaned
