with posts as (

select * from {{ ref('stg_tiktok_posts') }} posts ),
bucketed_posts as (

select *,
case 
    when likes is null then 'unknown'
    when likes between 0 and 50 then '0-50'
    when likes between 51 and 100 then '51-100'
    when likes between 101 and 200 then '101-200'
    when likes between 201 and 500 then '201-500'
    when likes between 501 and 1000 then '501-1000'
    when likes between 1001 and 5000 then '1001-5000'
    else '5000+'
end as like_bucket

from posts)

select
 post_id,
 post_date,
 posted_at,
 likes,
 sound,
 location,
 title,
 ingested_at,
 like_bucket
from bucketed_posts