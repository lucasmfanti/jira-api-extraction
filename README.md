# jira-api-extraction
This is a repo containing code for extracting data incrementally relative to issues from Jira API. The code is orchestrated in an Airflow DAG and feeds a raw table on an engine like BigQuery. Then a staging model for dbt selects the most recent data for each issue.
