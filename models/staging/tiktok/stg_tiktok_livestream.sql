-- models/staging/stg_tiktok_live_events.sql
-- Grain: one row per event captured during a TikTok live stream
-- Source: raw_tiktok_live_events (written by tiktok_live_ingestion.py)
--
-- Cleans and types the raw event stream:
--   - Deduplicates on event_id (idempotent, safe to run multiple times)
--   - Standardises ingested_at to UTC timestamp
--   - Derives event_date and event_hour for time-based analysis
--   - Nullifies empty strings for content and username
 
with source as (
    select * from {{ source('goshen_raw', 'raw_tiktok_live_events') }}
),
 
deduplicated as (
    select *,
        row_number() over (
            partition by event_id
            order by ingested_at asc
        ) as row_num
    from source
),
 
cleaned as (
    select
        event_id,
        event_type,
 
        -- Safe nulls for optional string fields
        nullif(trim(username), '')                          as username,
        nullif(trim(content), '')                           as content,
 
        -- Numeric value (like count, gift count, viewer count)
        value,
 
        stream_session_id,
 
        -- Timestamp standardisation
        cast(ingested_at as timestamp)                      as ingested_at,
 
        -- Derived time fields for mart aggregations
        date(ingested_at)                                   as event_date,
        extract(hour from ingested_at)                      as event_hour,
        extract(dayofweek from ingested_at)                 as day_of_week_num,
        case extract(dayofweek from ingested_at)
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end                                                 as day_of_week,
 
        -- Boolean flags for easy mart filtering
        event_type = 'comment'                              as is_comment,
        event_type = 'like'                                 as is_like,
        event_type = 'gift'                                 as is_gift,
        event_type = 'viewer_count'                         as is_viewer_count
 
    from deduplicated
    where row_num = 1
)
 
select * from cleaned