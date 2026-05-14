{{ config(
    materialized = 'incremental',
    unique_key   = 'comment_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_live_comments') }}

    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        -- Surrogate key: stable identifier for each comment
        to_hex(md5(concat(
            coalesce(room_id, ''), '|',
            coalesce(comment_time_raw, ''), '|',
            coalesce(comment_text, '')
        ))) as comment_id,

        room_id,

        -- Strip trailing " UTC" and cast
        cast(
            regexp_replace(comment_time_raw, r' UTC$', '')
            as timestamp
        )                  as comment_time,

        comment_text,
        ingested_at

    from source

)

select * from cleaned
