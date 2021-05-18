select
  p.campaign.id,
  p.campaign.name,
  p.campaign.optimizationScore as campaign_optimization_score,
  AVG(p.campaignBudget.amountMicros)/1000000 as budget,
  spend_7d,
  safe_divide(AVG(spend_7d), AVG(p.campaignBudget.amountMicros/1000000)) as budget_utilization
from `${datasetId}.report_base_campaigns` p
left join (
select
  c.id,
  c.name,
  AVG(costMicros)/1000000 as spend_7d,
from(
select 
  campaign.id,
  campaign.name,
  segments.date,
  SUM(metrics.costMicros) as costMicros
from `${datasetId}.report_base_campaign_performance` 
where date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
and segments.date < PARSE_DATE('%Y%m%d', '${partitionDay}')
and segments.date > date_sub(PARSE_DATE('%Y%m%d', '${partitionDay}'), interval 8 day)
group by 1, 2, 3
) c
group by 1,2
) t

on p.campaign.id = t.id
where date(_partitionTime) <= PARSE_DATE('%Y%m%d', '${partitionDay}')
and date(_partitionTime) > date_sub(PARSE_DATE('%Y%m%d', '${partitionDay}'), interval 7 day)
group by
  campaign.id,
  campaign.name,
  spend_7d,
  campaign_optimization_score
