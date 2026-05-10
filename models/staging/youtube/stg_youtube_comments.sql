with source as (
    select * from {{ source('analytics', 'youtube_comments') }}
),

renamed as (
    select
        video_id,
        coalesce(author, 'anonymous')                   as comment_author,
        coalesce(comment, '')                           as comment_text,
        coalesce(cast(like_count as int64), 0)          as like_count,
        cast(published_at as date)                      as published_at,
        cast(ingested_at as timestamp)                  as ingested_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by video_id, comment_author, published_at
            order by ingested_at desc
        ) as rn
    from renamed
)

select * except (rn) from deduped where rn = 1