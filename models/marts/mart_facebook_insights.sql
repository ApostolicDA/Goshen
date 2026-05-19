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

date_spine as (
    select date
    from unnest(
        generate_date_array(
            (select min(date) from pivoted),
            (select max(date) from pivoted),
            interval 1 day
        )
    ) as date
),

joined as (
    select
        d.date,
        format_date('%Y-%W', d.date)        as year_week,
        format_date('%Y-%m', d.date)        as year_month,
        format_date('%A', d.date)           as day_of_week,
        p.total_followers,
        coalesce(p.unique_impressions, 0)   as unique_impressions,
        coalesce(p.post_impressions, 0)     as post_impressions,
        coalesce(p.video_views, 0)          as video_views,
        coalesce(p.page_views, 0)           as page_views,
        coalesce(p.total_actions, 0)        as total_actions,
        coalesce(p.reaction_likes, 0)       as reaction_likes,
        coalesce(p.reaction_loves, 0)       as reaction_loves

    from date_spine d
    left join pivoted p on d.date = p.date
),

filled as (
    select
        date,
        year_week,
        year_month,
        day_of_week,

        last_value(total_followers ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        )                                   as total_followers,

        unique_impressions,
        post_impressions,
        video_views,
        page_views,
        total_actions,
        reaction_likes,
        reaction_loves

    from joined
),

final as (
    select
        *,
        reaction_likes + reaction_loves as total_reactions
    from filled
)

select * from final