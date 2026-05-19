-- models/marts/mart_social_followers_snapshot.sql

{{ config(materialized = 'table') }}

with youtube as (

    select
        'YouTube'                           as platform,
        cast(ingested_at as date)           as snapshot_date,
        cast(subscriber_count as int64)     as follower_count,
        cast(total_view_count as int64)     as total_views,
        cast(null as int64)                 as total_reach
    from {{ ref('stg_youtube_channel') }}
    qualify row_number() over (
        partition by cast(ingested_at as date)
        order by ingested_at desc
    ) = 1

),

tiktok as (

    select
        'TikTok'                            as platform,
        follow_date                         as snapshot_date,
        cast(cumulative_followers as int64) as follower_count,
        cast(null as int64)                 as total_views,
        cast(null as int64)                 as total_reach
    from {{ ref('mart_tiktok_followers') }}

),

facebook as (

    select
        'Facebook'                          as platform,
        date                                as snapshot_date,

        max(case when metric_name = 'page_follows'
            then cast(metric_value as int64) end)   as follower_count,

        max(case when metric_name = 'page_views_total'
            then cast(metric_value as int64) end)   as total_views,

        max(case when metric_name = 'page_impressions_unique'
            then cast(metric_value as int64) end)   as total_reach

    from {{ ref('stg_facebook_insights') }}
    where metric_name in (
        'page_follows',
        'page_views_total',
        'page_impressions_unique'
    )
    group by date

),

unioned as (

    select * from youtube
    union all select * from tiktok
    union all select * from facebook

),

with_growth as (

    select
        platform,
        snapshot_date,
        follower_count,
        total_views,
        total_reach,

        -- follower growth
        lag(follower_count) over (
            partition by platform
            order by snapshot_date
        )                                   as prev_period_followers,

        round(
            safe_divide(
                follower_count - lag(follower_count) over (
                    partition by platform order by snapshot_date
                ),
                lag(follower_count) over (
                    partition by platform order by snapshot_date
                )
            ) * 100
        , 1)                                as pct_growth_mom,

        -- views growth
        lag(total_views) over (
            partition by platform
            order by snapshot_date
        )                                   as prev_period_views,

        round(
            safe_divide(
                total_views - lag(total_views) over (
                    partition by platform order by snapshot_date
                ),
                lag(total_views) over (
                    partition by platform order by snapshot_date
                )
            ) * 100
        , 1)                                as pct_views_growth

    from unioned
    where platform is not null

)

select * from with_growth