WITH
  brand_reach AS (
    SELECT
      user_id,
      'Y' as TrueView,
      sum(IFNULL(advertiser_impression_cost_usd, 0)) AS cost_imp_usd,
      count(distinct query_id.time_usec) as impressions
    FROM
      adh.google_ads_impressions
    WHERE
      timestamp_micros(query_id.time_usec)
              between TIMESTAMP(@start_date, @time_zone)
              and TIMESTAMP(@end_date, @time_zone)
    AND
      campaign_id IN UNNEST(@brand_campaign_ids)
    AND user_id != '0'
    GROUP BY user_id
    ),
  brand_clk AS (
    SELECT
      user_id,
      sum(IFNULL(advertiser_click_cost_usd,0)) AS cost_clk_usd
    FROM
      adh.google_ads_clicks
    Where
      timestamp_micros(query_id.time_usec)
              between TIMESTAMP(@start_date, @time_zone)
              and TIMESTAMP(@end_date, @time_zone)
    AND impression_data.campaign_id IN UNNEST(@brand_campaign_ids)
    AND user_id != '0'
    GROUP BY user_id
    ),
  perf_reach AS (
    SELECT
      user_id,
      'Y' as AC,
      sum(IFNULL(advertiser_impression_cost_usd, 0)) AS cost_imp_usd,
      count(distinct query_id.time_usec) AS impressions
    FROM
      adh.google_ads_impressions
    WHERE 
      timestamp_micros(query_id.time_usec)
              between TIMESTAMP(@start_date, @time_zone)
              and TIMESTAMP(@end_date, @time_zone)
    AND
      campaign_id IN UNNEST(@perf_campaign_ids)
    AND user_id != '0'
    GROUP BY user_id
    ),
  perf_clk AS (
    SELECT
      user_id,
      sum(IFNULL(advertiser_click_cost_usd,0)) AS cost_clk_usd
    FROM
      adh.google_ads_clicks
    Where
      timestamp_micros(query_id.time_usec)
              between TIMESTAMP(@start_date, @time_zone)
              and TIMESTAMP(@end_date, @time_zone)
    AND impression_data.campaign_id IN UNNEST(@perf_campaign_ids)
    AND user_id != '0'
    GROUP BY user_id
    ),
    total_reach AS (
    select distinct user_id FROM (
        select user_id from brand_reach
        union all
        select user_id from perf_reach
    )
    ),
  master as (
    select
      tr.user_id,
      trv.TrueView,
      ac.AC,
      trv.impressions as brand_avg_freq,
      ac.impressions as ac_avg_freq,
      CASE
        WHEN (trv.TrueView = 'Y') then IFNULL(trv.cost_imp_usd, 0) + IFNULL(trvc.cost_clk_usd,0)
        else 0
      END
      as brand_cost_usd,
      CASE
        WHEN (ac.AC = 'Y') then IFNULL(ac.cost_imp_usd, 0) + IFNULL(acc.cost_clk_usd,0)
        else 0
      END
      as ac_cost_usd,
      CASE
        WHEN (trv.TrueView = 'Y') then IFNULL(trv.cost_imp_usd, 0) + IFNULL(trvc.cost_clk_usd,0)
        else 0
      END
      +
      CASE
        WHEN (ac.AC = 'Y') then IFNULL(ac.cost_imp_usd, 0) + IFNULL(acc.cost_clk_usd,0)
        else 0
      END
      as cost_usd,   
    FROM total_reach tr
    left outer join brand_reach trv on tr.user_id = trv.user_id
    left outer join brand_clk trvc on tr.user_id = trvc.user_id
    left outer join perf_reach ac on tr.user_id = ac.user_id
    left outer join perf_clk acc on tr.user_id = acc.user_id
    where ifnull(trv.impressions,0) <= 1000 -- remove frequency 1000+ outliers
    and ifnull(ac.impressions,0) <= 50 -- to control AC freq at similar level between the two groups
    ), 
  reach_result as (
    select
      'reach' as result,
      count(*) as total,
      sum (
        case
          when (TrueView = 'Y' and AC IS NULL) then 1 else 0
          end
      ) as branding_only,
      sum (
        case
          when (TrueView IS NULL and AC = 'Y') then 1 else 0
          end
      ) as performance_only,
      sum (
        case
          when (TrueView = 'Y' and AC = 'Y') then 1 else 0
          end
      ) as overlap
      FROM master
    ),
  brand_cost_result as (
    select
      'brand_cost_usd' as result,
      sum(ifnull(brand_cost_usd,0)) as total,
      sum (
        case
          when (TrueView = 'Y' and AC IS NULL) then brand_cost_usd else 0
          end
      ) as branding_only,
      sum (
        case
          when (TrueView IS NULL and AC = 'Y') then brand_cost_usd else 0
          end
      ) as performance_only,
      sum (
        case
          when (TrueView = 'Y' and AC = 'Y') then brand_cost_usd else 0
          end
      ) as overlap
      FROM master
    ),
  perf_cost_result as (
    select
      'ac_cost_usd' as result,
      sum(ifnull(ac_cost_usd,0)) as total,
      sum (
        case
          when (TrueView = 'Y' and AC IS NULL) then ac_cost_usd else 0
          end
      ) as branding_only,
      sum (
        case
          when (TrueView IS NULL and AC = 'Y') then ac_cost_usd else 0
          end
      ) as performance_only,
      sum (
        case
          when (TrueView = 'Y' and AC = 'Y') then ac_cost_usd else 0
          end
      ) as overlap
      FROM master
    ),
  cost_result as (
    select
      'cost_usd' as result,
      sum(ifnull(cost_usd,0)) as total,
      sum (
        case
          when (TrueView = 'Y' and AC IS NULL) then cost_usd else 0
          end
      ) as branding_only,
      sum (
        case
          when (TrueView IS NULL and AC = 'Y') then cost_usd else 0
          end
      ) as performance_only,
      sum (
        case
          when (TrueView = 'Y' and AC = 'Y') then cost_usd else 0
          end
      ) as overlap
      FROM master
    ),
  perf_freq_result as (
    select
      '_ac_freq' as result,
      avg(ifnull(ac_avg_freq,0)) as total,
      avg (
        case
          when (TrueView = 'Y' and AC IS NULL) then ifnull(ac_avg_freq,0) else null
          end
      ) as branding_only,
      avg (
        case
          when (TrueView IS NULL and AC = 'Y') then ifnull(ac_avg_freq,0) else null
          end
      ) as performance_only,
      avg (
        case
          when (TrueView = 'Y' and AC = 'Y') then ifnull(ac_avg_freq,0) else null
          end
      ) as overlap
      FROM master
    ),
  brand_freq_result as (
    select
      '_brand_freq' as result,
      avg(ifnull(brand_avg_freq,0)) as total,
      avg (
        case
          when (TrueView = 'Y' and AC IS NULL) then ifnull(brand_avg_freq,0) else null
          end
      ) as branding_only,
      avg (
        case
          when (TrueView IS NULL and AC = 'Y') then ifnull(brand_avg_freq,0) else null
          end
      ) as performance_only,
      avg (
        case
          when (TrueView = 'Y' and AC = 'Y') then ifnull(brand_avg_freq,0) else null
          end
      ) as overlap
      FROM master
    ) 

select *
from reach_result
UNION ALL
select *
from cost_result
UNION ALL
select *
from brand_cost_result
UNION ALL
select *
from perf_cost_result
UNION ALL
select *
from brand_freq_result
UNION ALL
select *
from perf_freq_result;
