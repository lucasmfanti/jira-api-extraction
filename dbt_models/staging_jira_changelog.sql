WITH stg AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY CONCAT(c.id, c.issue_id, c.user, c.entity, c.from, c.to, c.created_at) ORDER BY extracted_at DESC) AS row_num
  FROM
    {{source ('raw', 'jira_changelog') }}
)

SELECT 
  id,
  issue_id,
  user,
  entity,
  IF(from_string = "", NULL, from_string) AS from_string,
  IF(to_string = "", NULL, to_string) AS to_string,
  DATETIME(created_at, 'Your/Timezone') AS created_at,
  DATETIME(extracted_at, 'Your/Timezone') AS extracted_at
FROM stg
WHERE row_num = 1