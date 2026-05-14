with live_sessions as (

    select
        room_id,
        live_title,
        live_date,
        start_time,
        end_time,

        duration_mins,
        round(duration_mins / 60, 2) as duration_hours,

        format_date('%A', live_date) as live_day_name,

        total_views,
        total_gifters,
        total_likes,

        ingested_at,
        case
    when duration_mins < 30 then 'Short'
    when duration_mins < 90 then 'Medium'
    else 'Long'
end as live_duration_bucket,
Round(safe_divide(total_likes, total_views), 2) as like_rate

    from {{ ref('stg_tiktok_live') }}

)

select *
from live_sessions