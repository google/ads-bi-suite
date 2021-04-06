select
    distinct a.customer.id,
    b.label.name
  from `${datasetId}.report_base_customer_label` a

  left join
    `${datasetId}.report_base_labels` b
  on
    a.customerLabel.label = b.label.resourceName
  where DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  and DATE(b._partitionTime )= PARSE_DATE('%Y%m%d', '${partitionDay}')