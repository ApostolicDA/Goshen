with source as (
    select * from {{ source('analytics', 'facebook_posts') }}
),

renamed as (
    select
        post_id,
        coalesce(message, '')                       as post_message,
        cast(created_time as date)                  as post_date,
        coalesce(content_type, 'unknown')           as content_type,
        cast(ingested_at as timestamp)              as ingested_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by post_id
            order by ingested_at desc
        ) as rn
    from renamed
)

select * except (rn) from deduped where rn = 1