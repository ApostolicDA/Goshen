with source as (
    select * from {{ source('analytics', 'youtube_videos') }}
),

renamed as (
    select
        video_id,
        coalesce(title, 'unknown')                      as video_title,
        cast(published_at as date)                      as published_at,
        coalesce(cast(view_count as int64), 0)          as view_count,
        coalesce(cast(like_count as int64), 0)          as like_count,
        coalesce(cast(comment_count as int64), 0)       as comment_count,
        coalesce(cast(favorite_count as int64), 0)      as favorite_count,

        -- content metadata
        duration_raw,
        cast(duration_seconds as int64)                 as duration_seconds,
        lower(coalesce(live_broadcast_content, 'none')) as live_broadcast_content,
        lower(coalesce(definition, 'unknown'))          as definition,
        lower(coalesce(caption, 'false'))               as caption,
        category_id,
        default_language,
        tags_raw,

        cast(ingested_at as timestamp)                  as ingested_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by video_id
            order by ingested_at desc
        ) as rn
    from renamed
)

select * except (rn) from deduped where rn = 1