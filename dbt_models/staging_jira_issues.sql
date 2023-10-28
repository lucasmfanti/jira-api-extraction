WITH stg AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY extracted_at DESC) AS row_num
  FROM
    {{ source ('raw', 'jira_issues') }}
)

SELECT 
  id,
  key,
  project_key,
  IF(creator_name = "", NULL, creator_name) AS creator_name,
  IF(reporter_name = "", NULL, reporter_name) AS reporter_name,
  IF(assignee_name = "", NULL, assignee_name) AS assignee_name,
  IF(custom_field_1 = "", NULL, custom_field_1) AS custom_field_1,
  IF(summary = "", NULL, summary) AS summary,
  IF(status = "", NULL, status) AS status,
  IF(custom_field_2 = "", NULL, custom_field_2) AS custom_field_2,
  DATETIME(created_at, 'Your/Timezone') AS created_at,
  DATETIME(updated_at, 'Your/Timezone') AS updated_at,
  DATETIME(resolution_date, 'Your/Timezone') AS resolution_date,
  DATETIME(extracted_at 'Your/Timezone') AS extracted_at,
FROM stg
WHERE row_num = 1