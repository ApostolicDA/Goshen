with source as (
    select * from {{ source('analytics', 'youtube_channel') }}
),

renamed as (
    select
        channel_id,
        coalesce(channel_title, 'unknown')              as channel_title,
        coalesce(cast(subscriber_count as int64), 0)    as subscriber_count,
        coalesce(cast(view_count as int64), 0)          as total_view_count,
        coalesce(cast(video_count as int64), 0)         as total_video_count,
        cast(ingested_at as timestamp)                  as ingested_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by channel_id
            order by ingested_at desc
        ) as rn
    from renamed
)

select * except (rn) from deduped where rn = 1