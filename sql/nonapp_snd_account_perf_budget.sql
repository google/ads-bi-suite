select
  a.customer.descriptiveName as Account,
  a.customer.id as Customer_ID,
  cast(a.segments.date as DATE) as Day,
  a.customer.currencyCode as Currency,
  a.customer.optimizationScore as Account_optimization_score,
  b.accountBudget.adjustedSpendingLimitMicros/1000000 as Budget_approved,
  b.accountBudget.amountServedMicros/1000000 as Budget_served,
  b.accountBudget.approvedStartDateTime as Budget_start_time,
  b.accountBudget.approvedEndDateTime as Budget_end_time,
  (ifnull(cast(b.accountBudget.adjustedSpendingLimitMicros as INT64),0) - b.accountBudget.amountServedMicros)/1000000 as Budget_remain,
  avg((ifnull(cast(b.accountBudget.adjustedSpendingLimitMicros as INT64),0) - b.accountBudget.amountServedMicros)) / avg(nullif(a.metrics.costMicros,0)) as Budget_last,
  a.metrics.clicks as clicks,
  a.metrics.impressions as impressions,
  #a.metrics.ctr as CTR,
  #a.metrics_average_cpc as CPC,
  a.metrics.costMicros/1000000 as cost,
  a.metrics.conversions as conversions,
  a.metrics.costMicros/1000000/nullif(a.metrics.conversions,0) as CPA,
  a.metrics.conversionsValue as Conv_value,
  a.metrics.conversionsValue/nullif((a.metrics.costMicros/1000000),0) as ROI,
  a.metrics.allConversions as All_conversions,
  a.metrics.allConversionsValue as All_conv_value
from `${datasetId}.report_base_account_performance` a left join `${datasetId}.report_base_account_budget` b on a.customer.id = b.customer.id

where DATE(a._partitionTime) =  PARSE_DATE('%Y%m%d', '${partitionDay}')
and DATE(b._partitionTime) =  PARSE_DATE('%Y%m%d', '${partitionDay}')
and b.accountBudget.approvedStartDateTime is not null

and (b.accountBudget.approvedEndDateTime is null or cast(b.accountBudget.approvedEndDateTime as datetime) >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
group by
  Account,
  Customer_ID,
  Day,
  Currency,
  Account_optimization_score
  Budget_approved,
  Budget_served,
  Budget_start_time,
  Budget_end_time,
  Budget_remain,
  clicks,
  impressions,
  cost,
  conversions,
  CPA,
  Conv_value,
  ROI,
  All_conversions,
  All_conv_value
