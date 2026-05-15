with youtube as (
    select
        'youtube'                               as platform,
        (select max(subscriber_count) from {{ ref('stg_youtube_channel') }})    as total_followers,
        (select max(total_video_count) from {{ ref('stg_youtube_channel') }})   as total_posts,
        (select max(total_view_count) from {{ ref('stg_youtube_channel') }})    as total_views,
        sum(like_count)                                                          as total_likes,
        cast(null as int64)                                                      as total_live_sessions,
        round(avg(engagement_rate_pct), 2)                                       as avg_engagement_rate
    from {{ ref('mart_youtube_perfomance') }}
),
facebook as (
    select
        'facebook'                          as platform,
        max(total_followers)                as total_followers,
        (select count(*) from {{ ref('mart_facebook_posts') }})  as total_posts,
        max(unique_impressions)             as total_views,
        max(total_reactions)                as total_likes,
        cast(null as int64)                 as total_live_sessions,
        round(safe_divide(
            max(total_reactions),
            max(unique_impressions)
        ) * 100, 2)                         as avg_engagement_rate
    from {{ ref('mart_facebook_insights') }}
),

tiktok as (
    select
        'tiktok'                                as platform,
        (select max(cumulative_followers) from {{ ref('mart_tiktok_followers') }}) as total_followers,
        (select count(*) from {{ ref('mart_posts_perfomance') }}) as total_posts,
        sum(total_views)                        as total_views,
        sum(total_likes)                        as total_likes,
        count(*)                                as total_live_sessions,
        round(avg(like_rate), 2)                as avg_engagement_rate
    from {{ ref('mart_tiktok_live_perfomance') }}
)
select * from youtube
union all
select * from facebook
union all
select * from tiktok