{{ config(
    materialized = 'view'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_live_comments') }}

),

cleaned as (

    select
        to_hex(md5(concat(
            coalesce(room_id, ''), '|',
            coalesce(comment_time_raw, ''), '|',
            coalesce(comment_text, '')
        ))) as comment_id,

        room_id,

        cast(
            regexp_replace(comment_time_raw, r' UTC$', '')
            as timestamp
        )                  as comment_time,

        comment_text,
        ingested_at

    from source

)

select * from cleaned