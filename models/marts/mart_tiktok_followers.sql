with followers as (
    select * from {{ ref('stg_tiktok_followers') }}
),

aggregated as (
    select
        date(followed_at) as follow_date,
        count(*) as new_followers
    from followers
    group by 1
),

-- generate every date in the range
date_spine as (
    select date
    from unnest(
        generate_date_array(
            (select min(date(followed_at)) from followers),
            (select max(date(followed_at)) from followers),
            interval 1 day
        )
    ) as date
),

-- fill missing dates with 0
filled as (
    select
        d.date as follow_date,
        coalesce(a.new_followers, 0) as new_followers
    from date_spine d
    left join aggregated a on d.date = a.follow_date
),

cumulative as (
    select
        follow_date,
        new_followers,
        sum(new_followers) over (order by follow_date) as cumulative_followers
    from filled
)

select * from cumulative