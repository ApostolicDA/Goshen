{{ config(
    materialized = 'incremental',
    unique_key   = 'post_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_posts') }}

    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

deduped as (

    select *,
        row_number() over (
            partition by date_raw, sound_raw
            order by ingested_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        to_hex(md5(concat(
    coalesce(date_raw, ''), '|',
    coalesce(sound_raw, '')
))) as post_id,

        -- Parse "2025-03-01 12:00:00 UTC" → date and timestamp
        cast(
            regexp_extract(date_raw, r'^(\d{4}-\d{2}-\d{2})')
            as date
        )                                                as post_date,

        cast(
            regexp_replace(date_raw, r' UTC$', '')
            as timestamp
        )                                                as posted_at,

        -- Cast likes; 0 if N/A or missing
        coalesce(
            cast(nullif(likes_raw, 'N/A') as int64),
            0
        )                                                as likes,

        nullif(visibility_raw, 'N/A')                   as visibility,
        nullif(sound_raw,      'N/A')                   as sound,
        nullif(location_raw,   'N/A')                   as location,
        nullif(title_raw,      'N/A')                   as title,

        ingested_at

    from deduped
    where row_num = 1

)

select * from cleaned
