{{ config(
    materialized = 'incremental',
    unique_key   = 'follower_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_followers') }}

    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

deduped as (

    -- Same user could follow, unfollow, refollow — keep latest record per user+date
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