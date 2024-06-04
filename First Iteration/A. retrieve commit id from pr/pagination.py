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

    # TODO: change the email and token

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

dbs = os.listdir('../commits_per_pr')
to_analyze = ["db-split3.sql-pagination.csv", "db-split6.sql-pagination.csv", "db-split7.sql-pagination.csv"]
dbs = [x for x in dbs if x in to_analyze]
package_id = 0


query2 = '''query getOid($owner: String!, $repoName: String!, $pullRequest: Int!, $cursor: String!) {
                                  repository(owner: $owner, name: $repoName) {
                                    pullRequest(number: $pullRequest) {
                                      commits(first: 250, after: $cursor) {
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

#TODO: change the GITHUB token
headers = {"Authorization": "token [GIT_TOKEN]"}

for db in tqdm(dbs, leave=True, desc="dbs", position=0):
    data = pd.read_csv(f'../commits_per_pr/{db}')
    for index, row in data.iterrows():
        cursor = None
        try:
            i = 0
            while True:
                variables = {
                    "owner": row['owner'],
                    "repoName": row['repo'],
                    "pullRequest": row['pullrequest'],
                    "cursor": cursor if cursor else row['cursor']
                }

                response = requests.post('https://api.github.com/graphql',
                                         json={'query': query2, 'variables': variables}, headers=headers)
                json = response.json()

                result = ""
                for node in json['data']['repository']['pullRequest']['commits']['edges']:
                    oid = node['node']['commit']['oid']
                    date = node['node']['commit']['committedDate']
                    result += f"{row['project_id']},{row['pull_number']},{row['owner']},{row['repo']},{oid}\n"

                db_name = db.split('-pagination')[0]
                if not os.path.exists(f'../commits_per_pr/{db_name}.csv'):
                    with open(f'../commits_per_pr/{db_name}.csv', 'w') as f:
                        head = "project_id,pullrequest,owner,repo,commit\n"
                        f.write(head)
                        f.write(result)
                else:
                    with open(f'../commits_per_pr/{db_name}.csv', 'a') as f:
                        f.write(result)

                rate_limit = json['data']['rateLimit']['remaining']
                # rate_limit = 0
                if rate_limit == 0:
                    reset_time = str_to_datetime(json['data']['rateLimit']['resetAt'])
                    reset_time = utc_to_local(reset_time)
                    # reset_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=2)
                    wait_until_time_passed(utc_to_local(reset_time))

                if not json['data']['repository']['pullRequest']['commits']['pageInfo']['hasNextPage']:
                    break

                print(f'Page {i+1} done')
                cursor = json['data']['repository']['pullRequest']['commits']['pageInfo']['endCursor']


        except Exception as e:
            content = f'Subject: PR MINING\n\nTHE PROCESS IS INTERRUPTED DUE TO THE FOLLOWING ERROR:\n{e}\n{traceback.format_exc()}\n'
            send_email_notification(content)

content = f"Subject: PR MINING\n\nTHE PROCESS IS COMPLETED:\n\t- YUPPIE YEAH"
send_email_notification(content)
