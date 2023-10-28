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

    changelog_df = pd.DataFrame()

    j = 0

    while j <= total_issues:
    
        query = {
                    'jql': jql,
                    'maxResults':101,
                    'startAt':j,
                    'expand':"changelog"
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

            total_changes = resp['issues'][i]['changelog']['total']

            for j in range(total_changes):
                
                concomitant_changes = len(resp['issues'][i]['changelog']['histories'][j]['items'])
                
                for k in range(concomitant_changes):

                    data = {
                        'id': resp['issues'][i]['changelog']['histories'][j]['id'],
                        'issue_id': resp['issues'][i]['id'],
                        'user': resp['issues'][i]['changelog']['histories'][j]['author']['displayName'],
                        'entity': resp['issues'][i]['changelog']['histories'][j]['items'][k]['field'],
                        'from_string': resp['issues'][i]['changelog']['histories'][j]['items'][k]['fromString'],
                        'to_string': resp['issues'][i]['changelog']['histories'][j]['items'][k]['toString'],
                        'created_at': resp['issues'][i]['changelog']['histories'][j]['created']
                    }

                    data_list.append(data)

        df = pd.DataFrame(data_list)

        changelog_df = pd.concat([changelog_df, df], ignore_index = True)

        j = j + 100

    changelog_df['extracted_at'] = extracted_at

    filename = 'jira_changelog_%s.csv' % dt.date.today().strftime("%Y_%m_%d")

    changelog_df.to_csv(filename, quoting=csv.QUOTE_ALL, index = False)

    return 'success'

default_args = {
    'owner': 'you',
    'start_date': dt.datetime(2023, 6, 8, 0, 0, 0),
    'concurrency': 1,
    'email': 'youremail@yourprovider.com',
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG ('DI_jira_changelog',
         default_args=default_args,
         schedule_interval=@daily,
         catchup=False
         ) as dag:
      opr_run = PythonOperator(task_id='jira_extraction', python_callable = jira_extraction)