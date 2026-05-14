with facebook as (

    select
        'facebook' as platform,
        max(cast(value as int64)) as total_followers
    from {{ source('analytics', 'facebook') }}
    where metric = 'page_follows'

),

tiktok as (

    select
        'tiktok' as platform,
        max(cumulative_followers) as total_followers
    from {{ ref('mart_tiktok_followers') }}

),

youtube as (

    select
        'youtube' as platform,
        max(cast(subscriber_count as int64)) as total_followers
    from {{ source('analytics', 'youtube_channel') }}

)

select * from facebook
union all
select * from tiktok
union all
select * from youtube