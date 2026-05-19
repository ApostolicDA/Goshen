with facebook as (

    select
        'facebook'                          as platform,
        max(cast(value as int64))           as total_followers,
        0                                   as total_posts,
        0                                   as total_likes,
        0                                   as total_views,
        0                                   as total_comments,
        0.0                                 as avg_engagement_rate,
        0                                   as total_live_sessions
    from {{ source('analytics', 'facebook') }}
    where metric = 'page_follows'

),

tiktok as (

    select
        'tiktok'                            as platform,
        max(cumulative_followers)           as total_followers,
        0                                   as total_posts,
        0                                   as total_likes,
        0                                   as total_views,
        0                                   as total_comments,
        0.0                                 as avg_engagement_rate,
        0                                   as total_live_sessions
    from {{ ref('mart_tiktok_followers') }}

),

youtube as (

    select
        'youtube'                                   as platform,
        (
            select max(cast(subscriber_count as int64))
            from {{ source('analytics', 'youtube_channel') }}
        )                                           as total_followers,
        count(distinct video_id)                    as total_posts,
        sum(like_count)                             as total_likes,
        sum(view_count)                             as total_views,
        sum(comment_count)                          as total_comments,
        round(avg(engagement_rate_pct), 2)          as avg_engagement_rate,
        countif(live_broadcast_content = 'live')    as total_live_sessions
    from {{ ref('mart_youtube_videos') }}

)

select * from facebook
union all
select * from tiktok
union all
select * from youtube