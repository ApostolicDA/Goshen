with facebook as (
    select * from {{ ref('stg_facebook_insights') }}
),

pivoted as (
    select
        date,
        format_date('%Y-%W', date)          as year_week,
        format_date('%Y-%m', date)          as year_month,
        format_date('%A', date)             as day_of_week,

        max(case when metric_name = 'page_follows'              then metric_value end) as total_followers,
        max(case when metric_name = 'page_impressions_unique'   then metric_value end) as unique_impressions,
        max(case when metric_name = 'page_posts_impressions'    then metric_value end) as post_impressions,
        max(case when metric_name = 'page_video_views'          then metric_value end) as video_views,
        max(case when metric_name = 'page_views_total'          then metric_value end) as page_views,
        max(case when metric_name = 'page_total_actions'        then metric_value end) as total_actions,
        max(case when metric_name = 'reaction_like'             then metric_value end) as reaction_likes,
        max(case when metric_name = 'reaction_love'             then metric_value end) as reaction_loves

    from facebook
    group by 1, 2, 3, 4
),

final as (
    select
        *,
        coalesce(reaction_likes, 0) + coalesce(reaction_loves, 0) as total_reactions
    from pivoted
)

select * from final