with videos as (

    select * from {{ ref('stg_youtube_videos') }}

),

comment_aggregates as (

    select
        video_id,
        count(*)                        as total_comments_ingested,
        sum(like_count)                 as total_comment_likes,
        max(published_at)               as latest_comment_date
    from {{ ref('stg_youtube_comments') }}
    group by video_id

),

final as (

    select
        -- identifiers
        v.video_id,
        v.video_title,
        v.published_at,
        format_date('%Y-%m', v.published_at)              as year_month,
        format_date('%A', v.published_at)                 as day_of_week,
        cast(format_date('%u', v.published_at) as int64)  as day_of_week_num,

        -- performance
        v.view_count,
        v.like_count,
        v.comment_count,
        v.favorite_count,

        -- engagement rate
        round(safe_divide(
            v.like_count + v.comment_count,
            v.view_count
        ) * 100, 2)                                       as engagement_rate_pct,

        -- content type
        case
            when v.live_broadcast_content = 'live' then 'Live Stream'
            else 'Uploaded'
        end                                               as content_type,

        -- duration
        round(v.duration_seconds / 60, 1)                 as duration_minutes,
        case
            when v.duration_seconds is null               then 'Unknown'
            when v.duration_seconds < 60                  then 'Shorts (< 1 min)'
            when v.duration_seconds < 1800                then 'Short (< 30 min)'
            when v.duration_seconds < 3600                then 'Medium (< 60 min)'
            else                                               'Long (60+ min)'
        end                                               as duration_bucket,

        -- comment aggregates (no row multiplication)
        coalesce(c.total_comments_ingested, 0)            as total_comments_ingested,
        coalesce(c.total_comment_likes, 0)                as total_comment_likes,
        c.latest_comment_date,

        -- metadata
        v.live_broadcast_content,
        v.tags_raw,
        v.definition,
        v.caption,
        v.category_id,
        v.default_language,

        v.ingested_at

    from videos v
    left join comment_aggregates c using (video_id)

)

select * from final