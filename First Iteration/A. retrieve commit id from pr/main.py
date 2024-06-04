import os
import smtplib
import sqlite3
import time
import datetime
import traceback

import pandas as pd
import pytz
import requests
from tqdm import tqdm


def send_email_notification(content):
    mail = smtplib.SMTP('smtp.gmail.com', 587)

    mail.ehlo()

    mail.starttls()

    #TODO: change the email and token

    mail.login('[EMAIL]', '[TOKEN EMAIL]]')

    mail.sendmail('[FROM_EMAIL]', '[TO_EMAIL]', content)

    mail.close()


def wait_until_time_passed(target_time):
    current_time = utc_to_local(datetime.datetime.utcnow().replace(tzinfo=None))
    diff_minutes = time_diff_minutes(current_time, target_time)
    content = f'Subject: PR MINING\n\nTHE PROCESS IS PAUSED DUE TO THE RATE LIMIT:\n\t- Waiting: {diff_minutes:.2f} minutes'
    send_email_notification(content)

    if diff_minutes > 0:
        # print(f"Waiting for {diff_minutes:.2f} minutes...")
        for _ in tqdm(range(int(diff_minutes)), desc="Waiting", unit="min", position=2, leave=False):
            time.sleep(60)  # Wait for 1 minute
    content = f'Subject: PR MINING\n\nTHE PROCESS IS RESUMED AFTER THE RATE LIMIT'
    send_email_notification(content)


# Convert string to datetime object
def str_to_datetime(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")


# Convert UTC datetime to local datetime
def utc_to_local(dt):
    return dt.replace(tzinfo=pytz.utc).astimezone(tz=None)


# Calculate time difference in minutes
def time_diff_minutes(dt1, dt2):
    diff = dt2 - dt1
    return diff.total_seconds() / 60


query = """     SELECT DISTINCT gh_pull.project_id, gh_pull.pull_number, projects.owner, projects.project
                FROM main.gh_pull
                JOIN projects ON gh_pull.project_id = projects.id;
            """

dbs = os.listdir('../dbs')
dbs = [x for x in dbs if x.endswith('.sql')]
dbs = [x for x in dbs if x.startswith('db-split')]
to_analyze = ["db-split3.sql", "db-split7.sql"]
dbs = [x for x in dbs if x in to_analyze]
package_id = 0

for db in tqdm(dbs, position=0, leave=True, desc="Processing databases"):
    try:
        already_processed = pd.DataFrame()
        # if os.path.exists(f'../commits_per_pr/{db}.csv'):
        #     already_processed = pd.read_csv(f'../commits_per_pr/{db}.csv')
        #     already_processed = already_processed[
        #         ["project_id", "pullrequest", "owner", "repo", "commit"]].drop_duplicates()
        #     already_processed = already_processed[already_processed['project_id'] != 'project_id']
        #     already_processed = already_processed[['project_id', 'pullrequest']].drop_duplicates()
        #     already_processed.rename(columns={'pullrequest': 'pull_number'}, inplace=True)
        #     already_processed['project_id'] = already_processed['project_id'].astype(int)
        #     already_processed['pull_number'] = already_processed['pull_number'].astype(int)

        # Connect to SQLite database
        conn = sqlite3.connect(f'../dbs/{db}')
        data = pd.read_sql_query(query, conn)

        data['project_id'] = data['project_id'].astype(int)
        data['pull_number'] = data['pull_number'].astype(int)

        if not already_processed.empty:

            # Merge 'data' with 'already_processed_unique' on 'project_id' and 'pull_number'
            merged_data = pd.merge(data, already_processed, on=['project_id', 'pull_number'], how='left',
                                   indicator=True)

            # Filter rows where both 'project_id' and 'pull_number' are not in 'already_processed_unique'
            filtered_data = merged_data[merged_data['_merge'] == 'left_only']

            # Drop the '_merge' column from the filtered data
            filtered_data = filtered_data.drop(columns=['_merge'])

            groups = filtered_data.groupby(['project_id', 'pull_number'])

        else:
            groups = data.groupby(['project_id', 'pull_number'])

        try:
            for name, group in tqdm(groups, position=1, leave=False, desc="Processing pull requests"):
                result = ""
                pull_request = group['pull_number'].iloc[0]

                query2 = '''query getOid($owner: String!, $repoName: String!, $pullRequest: Int!) {
                              repository(owner: $owner, name: $repoName) {
                                pullRequest(number: $pullRequest) {
                                  commits(first: 250) {
                                    totalCount
                                    pageInfo {
                                        endCursor
                                        hasNextPage
                                    }
                                    edges {
                                      node {
                                        commit {
                                          oid
                                          committedDate
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              rateLimit {
                                cost
                                limit
                                remaining
                                used
                                resetAt
                              }
                            }'''

                variables = {
                    "owner": group['owner'].iloc[0],
                    "repoName": group['project'].iloc[0],
                    "pullRequest": int(pull_request)
                }

                #TODO: change the GITHUB token
                headers = {"Authorization": "token [GIT_TOKEN]"}

                response = requests.post('https://api.github.com/graphql',
                                         json={'query': query2, 'variables': variables}, headers=headers)
                json = response.json()

                if json['data']['repository']['pullRequest']['commits']['totalCount'] > 250 or \
                        json['data']['repository']['pullRequest']['commits']['pageInfo']['hasNextPage'] is True:

                    cursor = json['data']['repository']['pullRequest']['commits']['pageInfo']['endCursor']
                    tot_count = json['data']['repository']['pullRequest']['commits']['totalCount']

                    if not os.path.exists(f'../commits_per_pr/{db}-pagination.csv'):

                        with open(f'../commits_per_pr/{db}-pagination.csv', 'w') as f:
                            head = "project_id,pullrequest,owner,repo,tot_count,cursor\n"
                            result = f"{group['project_id'].iloc[0]},{group['pull_number'].iloc[0]},{group['owner'].iloc[0]},{group['project'].iloc[0]},{tot_count},{cursor}\n"
                            f.write(head)
                            f.write(result)
                    else:
                        with open(f'../commits_per_pr/{db}-pagination.csv', 'a') as f:
                            result = f"{group['project_id'].iloc[0]},{group['pull_number'].iloc[0]},{group['owner'].iloc[0]},{group['project'].iloc[0]},{tot_count},{cursor}\n"
                            f.write(result)


                for node in json['data']['repository']['pullRequest']['commits']['edges']:
                    oid = node['node']['commit']['oid']
                    date = node['node']['commit']['committedDate']
                    result += f"{group['project_id'].iloc[0]},{group['pull_number'].iloc[0]},{group['owner'].iloc[0]},{group['project'].iloc[0]},{oid}\n"

                if not os.path.exists(f'../commits_per_pr/{db}.csv'):
                    with open(f'../commits_per_pr/{db}.csv', 'w') as f:
                        head = "project_id,pullrequest,owner,repo,commit\n"
                        f.write(head)
                        f.write(result)
                else:
                    with open(f'../commits_per_pr/{db}.csv', 'a') as f:
                        f.write(result)

                rate_limit = json['data']['rateLimit']['remaining']
                # rate_limit = 0
                if rate_limit == 0:
                    reset_time = str_to_datetime(json['data']['rateLimit']['resetAt'])
                    reset_time = utc_to_local(reset_time)
                    # reset_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=2)
                    wait_until_time_passed(utc_to_local(reset_time))

        except Exception as e:
            content = f'Subject: PR MINING\n\nTHE PROCESS IS INTERRUPTED DUE TO THE FOLLOWING ERROR:\n{e}\n{traceback.format_exc()}\n\t- Analyzing dbs: {db},\n\t- PR: {group["pull_number"].iloc[0]},\n\t- Project: {data["project"].iloc[0]},\n\t- Owner: {data["owner"].iloc[0]}'
            send_email_notification(content)

    except Exception as e:
        content = f'Subject: PR MINING\n\nTHE PROCESS IS INTERRUPTED DUE TO THE FOLLOWING ERROR:\n{e}\n{traceback.format_exc()}\n\t- Analyzing dbs: {db}'
        send_email_notification(content)

    else:
        content = f"Subject: PR MINING\n\nTHE PROCESS IS COMPLETED:\n\t- YUPPIE YEAH"
        send_email_notification(content)
