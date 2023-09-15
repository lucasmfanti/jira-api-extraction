import requests
import json
import pytz
import csv
import pandas as pd
import datetime as dt
from requests.auth import HTTPBasicAuth

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def jira_extraction():

  url = "https://yourdomain.atlassian.net/rest/api/3/search"

  token = "xxxtokenxxx"

  auth = HTTPBasicAuth("youremail@yourprovider.com",
                      f"{token}")

  headers = {
    "Accept": "application/json"
  }

  now = dt.datetime.now(pytz.timezone('Your/Timezone'))
  yesterday_tmp = now - dt.timedelta(days = 1)
  yesterday = yesterday_tmp.strftime('%Y-%m-%d')

  jql = f'project = "Your Project" AND updated >=  "{yesterday}"'

  query = {
              'jql': jql,
              'maxResults':101,
              'startAt':0
        }

  response = requests.request(
      "GET",
      url,
      headers=headers,
      params=query,
      auth=auth
  )

  resp = response.json()

  total_issues = resp['total']

  extracted_at = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + now.strftime('%z')

  issues_df = pd.DataFrame()

  j = 0

  while j <= total_issues:
    query = {
                'jql': jql,
                'maxResults':101,
                'startAt':j
          }

    response = requests.request(
        "GET",
        url,
        headers=headers,
        params=query,
        auth=auth
    )

    resp = response.json()

    num_issues = len(resp['issues'])
    data_list = []

    for i in range(num_issues):
        data = {
          'id': resp['issues'][i]['id'],
          'key': resp['issues'][i]['key'],
          'project_key': resp['issues'][i]['fields']['project']['key'] if resp['issues'][i]['fields']['project'] is not None else None,
          'creator_name': resp['issues'][i]['fields']['creator']['displayName'] if resp['issues'][i]['fields']['creator'] is not None else None,
          'reporter_name': resp['issues'][i]['fields']['reporter']['displayName'] if resp['issues'][i]['fields']['reporter'] is not None else None,
          'assignee_name': resp['issues'][i]['fields']['assignee']['displayName'] if resp['issues'][i]['fields']['assignee'] is not None else None,
          'custom_field_1': resp['issues'][i]['fields']['customfield_1'],
          'summary': resp['issues'][i]['fields']['summary'],
          'status': resp['issues'][i]['fields']['status']['name'] if resp['issues'][i]['fields']['status'] is not None else None,
          'custom_field_2': resp['issues'][i]['fields']['customfield_2']['value'] if resp['issues'][i]['fields']['customfield_2'] is not None else None,
          'created_at': resp['issues'][i]['fields']['created'],
          'updated_at': resp['issues'][i]['fields']['updated'],
          'resolution_date': resp['issues'][i]['fields']['resolutiondate']
        }

        data_list.append(data)

    df = pd.DataFrame(data_list)

    issues_df = pd.concat([issues_df, df], ignore_index = True)

    j = j + 100

  issues_df['extracted_at'] = extracted_at

  filename = 'jira_issues_%s.csv' % dt.date.today().strftime("%Y_%m_%d")

  issues_df.to_csv(filename, quoting=csv.QUOTE_ALL, index = False)

  return 'success'

default_args = {
    'owner': 'you',
    'start_date': dt.datetime(2023, 6, 8, 0, 0, 0),
    'concurrency': 1,
    'email': 'youremail@yourprovider.com',
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG ('jira_issues',
         default_args=default_args,
         schedule_interval=@daily,
         catchup=False
         ) as dag:
      opr_run = PythonOperator(task_id='jira_extraction', python_callable = jira_extraction)