with videos as (

    select * from {{ ref('stg_youtube_videos') }}

),

comments as (

    select * from {{ ref('stg_youtube_comments') }}

),

joined as (

    select
        v.video_id,
        v.video_title,
        v.published_at,
        v.view_count,
        v.like_count,
        v.comment_count,
        c.comment_author,
        c.comment_text,
        c.published_at     as comment_date,
        c.like_count       as comment_likes
    from videos v
    left join comments c on v.video_id = c.video_id

)

select * from joined