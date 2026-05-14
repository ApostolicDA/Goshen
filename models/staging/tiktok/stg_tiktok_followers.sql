{{ config(
    materialized = 'view'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_followers') }}

),

deduped as (

    select *,
        row_number() over (
            partition by username, followed_at_raw
            order by ingested_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        to_hex(md5(concat(
            coalesce(username, ''), '|',
            coalesce(followed_at_raw, '')
        ))) as follower_id,

        cast(
            regexp_replace(followed_at_raw, r' UTC$', '')
            as timestamp
        )                  as followed_at,

        username,
        ingested_at

    from deduped
    where row_num = 1

)

select * from cleaned