# jira-api-extraction
This is a repo containing code for extracting data incrementally relative to issues from Jira API. There also is an extraction for the changelog history of the issue. The code is orchestrated in an Airflow DAG and feeds a raw table on an engine like BigQuery. Then a staging model for dbt selects the most recent data for each issue and changelog.
# What is Jira?
Jira is a powerful project management and issue tracking software developed by Atlassian. It is widely utilized by teams and organizations across various industries to streamline and manage their projects, tasks, and workflows efficiently. Jira offers a comprehensive suite of tools for creating and prioritizing tasks, assigning them to team members, tracking progress, and collaborating on projects. Its flexibility allows it to adapt to a wide range of use cases, from software development and IT support to marketing and business operations.
# The DAGs
The [Issues DAG](airflow_dags/jira_issues.py) is written for Airflow with a daily schedule. It always looks for data on issues that have been updated during the day before. The csv generated is then uploaded to Google Cloud Storage, with a name containing the date of the extraction. A live table is created on Google BigQuery then with all the csv files from the bucket. 
The [Changelog DAG](airflow_dags/jira_changelog.py) works in a similar manner, running daily and capturing only data from the issues that were updated on the day before. The ID is NOT a primary key for the changelog table, as sometimes there is more than one entity changed with the same transition, so the combination of the ID and the Entity will be unique.
# The dbt Model
The [Issues Model](dbt_models/staging_jira_issues.sql) is a SQL code where dbt will run to update the raw table to a better suited for any other downstream calculations and dashboards feeding processes. It handles the problem to have only the most recent version of your data for each issue, guaranteeing that your id column will be a primary key for the table and a unique identifier, as well as handling Null values. The [Changelog Model](dbt_models/staging_jira_changelog.sql) is similar in function, dealing with Null values and guaranteeing that the combination of the ID and the Entity are unique when used together.