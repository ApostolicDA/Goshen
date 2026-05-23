-- mart_tiktok_post_vs_live.sql
with posts as (
    select
        post_date,
        count(*)        as posts_on_day,
        sum(likes)      as total_post_likes,
        avg(likes)      as avg_post_likes
    from {{ ref('mart_posts_perfomance') }}
    group by 1
),

live as (
    select
        live_date,
        count(*)         as live_sessions_on_day,
        sum(total_views) as live_views_on_day,
        sum(total_likes) as live_likes_on_day
    from {{ ref('mart_tiktok_live_perfomance') }}
    group by 1
),

joined as (
    select
        coalesce(p.post_date, l.live_date)      as activity_date,

        -- day fields
        format_date('%A', coalesce(p.post_date, l.live_date))   as day_of_week,
        format_date('%u', coalesce(p.post_date, l.live_date))   as day_number,

        coalesce(p.posts_on_day, 0)             as posts_on_day,
        coalesce(p.total_post_likes, 0)         as total_post_likes,
        coalesce(p.avg_post_likes, 0)           as avg_post_likes,
        coalesce(l.live_sessions_on_day, 0)     as live_sessions_on_day,
        coalesce(l.live_views_on_day, 0)        as live_views_on_day,
        coalesce(l.live_likes_on_day, 0)        as live_likes_on_day,

        case
            when l.live_date is not null
             and p.post_date is not null         then 'Post + Live'
            when l.live_date is not null         then 'Live Only'
            when p.post_date is not null         then 'Post Only'
        end                                     as activity_type

    from posts p
    full outer join live l on p.post_date = l.live_date
)

select * from joined