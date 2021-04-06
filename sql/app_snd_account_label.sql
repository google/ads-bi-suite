SELECT
    DISTINCT a.customer.id,
    b.label.name
FROM
    `${datasetId}.report_base_customer_label` a
    LEFT JOIN
    `${datasetId}.report_base_labels` b
    ON
    a.customerLabel.label = b.label.resourceName
WHERE
    DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
    AND DATE(b._partitionTime )= PARSE_DATE('%Y%m%d', '${partitionDay}')