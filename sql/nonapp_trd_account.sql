select
distinct
  Account,
  a.Customer_ID,

  Day,
  Currency,
  Budget_approved,
  Budget_served,
  Budget_start_time,
  Budget_end_time,
  Budget_remain,
  CASE
    when Budget_start_time is not null then Budget_last
    when Budget_start_time is null then 0
    else 0
  END Budget_last_days,
  clicks as Clicks,
  w.clicks_wow as Clicks_WOW,
  impressions as Impressions,
  cost as Cost,
  week1_cost as Week1_cost,
  week2_cost as Week2_cost,
  week1_clicks as Week1_clicks,
  week2_clicks as Week2_clicks,
  week1_conversions as Week1_conversions,
  week2_conversions as Week2_conversions,
  week1_conversion_value as Week1_conversion_value,
  week2_conversion_value as Week2_conversion_value,
  week1_all_conversion_value as Week1_all_conversion_value,
  week2_all_conversion_value as Week2_all_conversion_value,
  w.cost_wow as Cost_WOW,
  conversions as Conversions,
  w.conversions_wow as Conversions_WOW,
  Conv_value,
  All_conversions,
  All_conv_value
from ${datasetId}.nonapp_snd_account_perf_budget a
left join ${datasetId}.nonapp_snd_account_perf_cost_wow w on a.Customer_ID = w.Customer_id
