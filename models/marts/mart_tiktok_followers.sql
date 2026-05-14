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

cumulative as (

    select
        follow_date,
        new_followers,
        sum(new_followers) over (order by follow_date) as cumulative_followers
    from aggregated

)

select * from cumulative