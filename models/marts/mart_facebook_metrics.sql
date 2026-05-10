with facebook as (
    select * from {{ ref('stg_facebook_insights') }}
),

final as (
    select
        date,
        format_date('%Y-%W', date)          as year_week,
        format_date('%Y-%m', date)          as year_month,
        format_date('%A', date)             as day_of_week,

        metric_name,
        metric_value,

        case
            when metric_name in (
                'page_impressions_unique',
                'page_posts_impressions')           then 'reach'
            when metric_name in (
                'reaction_like',
                'reaction_love')                    then 'engagement'
            when metric_name = 'page_video_views'  then 'video'
            when metric_name = 'page_follows'       then 'growth'
            when metric_name in (
                'page_views_total',
                'page_total_actions')               then 'activity'
            else 'other'
        end                                 as metric_category,

        ingested_at

    from facebook
)

select * from final