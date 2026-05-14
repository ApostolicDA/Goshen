{{ config(
    materialized = 'incremental',
    unique_key   = 'watch_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (

    select *
    from {{ source('analytics', 'tiktok_watch_live_history') }}

    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

deduped as (

    select *,
        row_number() over (
            partition by watched_at_raw, link_raw
            order by ingested_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        to_hex(md5(concat(
    coalesce(watched_at_raw, ''), '|',
    coalesce(link_raw, '')
))) as watch_id,

        cast(watched_at_raw as timestamp)               as watched_at,

        nullif(link_raw, '')                            as link,

        -- Count how many comment lines are in the blob
        -- Each line starts with "[YYYY-MM-DD" so count those brackets
        array_length(
            regexp_extract_all(
                coalesce(raw_comments_blob, ''),
                r'\[\d{4}-\d{2}-\d{2}'
            )
        )                                               as comments_sent,

        raw_comments_blob                               as comments_raw,

        ingested_at

    from deduped
    where row_num = 1

)

select * from cleaned