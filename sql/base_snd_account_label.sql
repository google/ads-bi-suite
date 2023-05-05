-- Copyright 2023 Google LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT DISTINCT a.customer.id, b.label.name
FROM `${datasetId}.report_base_customer_label` a
LEFT JOIN `${datasetId}.report_base_labels` b
  ON a.customer_label.label = b.label.resource_name
WHERE
  DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND DATE(b._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')