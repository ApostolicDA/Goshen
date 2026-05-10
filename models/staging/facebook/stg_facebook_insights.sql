with source as (
    select * from {{ source('analytics', 'facebook') }}
),

renamed as (
    select
        cast(DATE as date)                                  as date,
        coalesce(METRIC, 'unknown')                         as metric_name,
        coalesce(cast(VALUE as string), '0')                as metric_value,
        cast(ingested_at as timestamp)                      as ingested_at
    from source
),

-- all normal numeric metrics (exclude reactions)
numeric_metrics as (
    select
        date,
        metric_name,
        cast(metric_value as int64)                         as metric_value,
        ingested_at
    from renamed
    where metric_name != 'page_actions_post_reactions_total'
),

-- reactions only — parse JSON and unpivot into rows
reactions as (
    select
        date,
        concat('reaction_', reaction_type)                  as metric_name,
        cast(reaction_value as int64)                       as metric_value,
        ingested_at
    from renamed,
    unnest([
        struct('like' as reaction_type,  cast(JSON_VALUE(replace(replace(metric_value, "'", '"'), 'None', 'null'), '$.like') as string) as reaction_value),
        struct('love' as reaction_type,  cast(JSON_VALUE(replace(replace(metric_value, "'", '"'), 'None', 'null'), '$.love') as string) as reaction_value)
    ])
    where metric_name = 'page_actions_post_reactions_total'
),

-- combine both back together
combined as (
    select * from numeric_metrics
    union all
    select * from reactions
),

deduped as (
    select *,
        row_number() over (
            partition by date, metric_name
            order by ingested_at desc
        ) as rn
    from combined
)

select * except (rn) from deduped where rn = 1