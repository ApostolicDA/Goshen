{{ config(materialized='table') }}
with posts as (
    select * from {{ ref('stg_facebook_posts') }}
),

final as (
    select
        post_id,
        post_date,
        format_date('%Y-%m', post_date)         as year_month,
        format_date('%A', post_date)            as day_of_week,
        format_date('%Y', post_date)            as year,

        post_message,

        -- content type categorization
        content_type,
        case
            when content_type = 'video_inline'  then 'video'
            when content_type = 'album'         then 'album'
            when content_type = 'photo'         then 'photo'
            when content_type = 'share'         then 'share'
            when content_type = 'native_templates' then 'link'
            else 'other'
        end                                     as content_category,

        -- post type (sermon recap, announcement, live, etc.)
        case
            when lower(post_message) like '%recap%'
              or lower(post_message) like '%sunday service%'
              or lower(post_message) like '%ministered by%'
              or lower(post_message) like '%title:%'       then 'sermon_recap'
            when lower(post_message) like '%we are live%'
              or lower(post_message) like '%join us live%'
              or lower(post_message) like '%live tonight%' then 'live_announcement'
            when lower(post_message) like '%join us%'
              or lower(post_message) like '%see you%'
              or lower(post_message) like '%you are invited%' then 'announcement'
            when lower(post_message) like '%happy birthday%'
              or lower(post_message) like '%we celebrate%'  then 'celebration'
            else 'general'
        end                                     as post_category,

        ingested_at

    from posts
)

select * from final