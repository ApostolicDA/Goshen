with videos as (
    select * from {{ ref('stg_youtube_videos') }}
),

comments as (
    select
        video_id,
        count(*)                        as total_comments,
        sum(like_count)                 as total_comment_likes,
        max(published_at)               as latest_comment_date
    from {{ ref('stg_youtube_comments') }}
    group by video_id
),

final as (
    select
        v.video_id,
        v.video_title,
        v.published_at,
        format_date('%Y-%m', v.published_at)        as year_month,
        format_date('%A', v.published_at)           as day_of_week,

        -- performance
        v.view_count,
        v.like_count,
        v.comment_count,

        -- engagement rate
        round(safe_divide(
            v.like_count + v.comment_count,
            v.view_count
        ) * 100, 2)                                 as engagement_rate_pct,

        -- comments detail from comments table
        coalesce(c.total_comments, 0)               as total_comments_ingested,
        coalesce(c.total_comment_likes, 0)          as total_comment_likes,
        c.latest_comment_date,

        -- content age
        date_diff(current_date(), v.published_at, day)  as days_since_published,

        v.ingested_at

    from videos v
    left join comments c using (video_id)
)

select * from final