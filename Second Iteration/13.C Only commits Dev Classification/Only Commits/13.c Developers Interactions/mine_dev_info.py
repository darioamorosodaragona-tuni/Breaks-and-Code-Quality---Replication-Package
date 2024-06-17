

import datetime
import os
import smtplib
import sqlite3
import time
import pandas as pd
import pytz
import requests
import json
from tqdm import tqdm
import traceback


def send_email_notification(content):
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()

    mail.login('[MAIL USED TO SEND]', '[GMAIL TOKEN]')
    mail.sendmail('[FROM]]', '[TO]', content)

    mail.close()


def wait_until_time_passed(target_time):
    current_time = utc_to_local(datetime.datetime.utcnow().replace(tzinfo=None))
    diff_minutes = time_diff_minutes(current_time, target_time)
    content = f'Subject: USER MINING\n\nTHE PROCESS IS PAUSED DUE TO THE RATE LIMIT:\n\t- Waiting: {diff_minutes:.2f} minutes'
    send_email_notification(content)

    if diff_minutes > 0:
        for _ in tqdm(range(int(diff_minutes)), desc="Waiting", unit="min", position=2, leave=False):
            time.sleep(60)  # Wait for 1 minute
    content = f'Subject: USER MINING\n\nTHE PROCESS IS RESUMED AFTER THE RATE LIMIT'
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

def load_processed_users(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(line.strip() for line in file)
    return set()

def save_processed_user(file_path, user_project):
    with open(file_path, 'a') as file:
        file.write(f"{user_project}\n")


os.makedirs("mining_results", exist_ok=True)
data = os.listdir("Data/dbs")
data = [x for x in data if x.endswith('.sql')]
GITHUB_TOKEN = "[GITHUB TOKEN]"
processed_users_file = "processed_users.txt"
processed_users = load_processed_users(processed_users_file)


for file in tqdm(data, desc="Analyzing dbs", unit="db", position=0, leave=True):
        db = sqlite3.connect(f"Data/dbs/{file}")
        sql_query = '''SELECT 
    gp.id AS project_id,
    gp.project,
    gp.owner,
    ghu.login,
    ghu.name,
    ghu.email,
    gu.id AS git_user_id
FROM 
    projects gp
JOIN 
    git_commit gc ON gp.id = gc.project_id
JOIN 
    git_users gu ON gc.git_user_id = gu.id
JOIN 
    gh_users ghu ON gu.author_email = ghu.email
WHERE 
    gu.author_name = ghu.name;
    '''

        user_data = pd.read_sql(sql_query, db)
        db.close()

        grouped_user_data = user_data.groupby(['project_id', 'project', 'owner', 'login', 'name', 'email'])
        unique_user_data = grouped_user_data['git_user_id'].unique().reset_index()

        for index, row in tqdm(unique_user_data.iterrows(), desc="Analyzing Users", unit="users", position=1,
                               leave=False):
            project_id = row['project_id']
            author = row['login']
            user_project_key = f"{project_id}_{author}"
            if user_project_key in processed_users:
                continue  # Skip already processed user-project combination

            try:
                AUTHOR_NME = row['name']
                AUTHOR_EMAIL = row['email']
                REPO_OWNER = row['owner']
                REPO_NAME = row['project']
                USER_IDS = row['git_user_id']
                AUTHOR = row['login']

                headers = {
                    'Authorization': f'bearer {GITHUB_TOKEN}',
                    'Content-Type': 'application/json'
                }

                # Step 1: Fetch user ID
                query_user_id = """
                {
                  user(login: "%s") {
                    id
                  }
                }
                """ % AUTHOR

                response = requests.post('https://api.github.com/graphql', headers=headers,
                                         data=json.dumps({"query": query_user_id}))
                user_id = response.json()['data']['user']['id']

                # Step 2: Use user ID in main query
                query_main = f"""
                query userPR_ISSUE_Info {{
                  repository(owner: "{REPO_OWNER}", name: "{REPO_NAME}") {{
                    commit: object(expression: "HEAD") {{
                      ... on Commit {{
                        history(author: {{ id: "{user_id}" }}) {{
                          totalCount
                        }}
                      }}
                    }}
                    issue_assigned: issues(first: 100, filterBy: {{assignee: "{AUTHOR}"}}) {{
                      totalCount
                    }}
                    issue_created: issues(first: 100, filterBy: {{createdBy: "{AUTHOR}"}}) {{
                      totalCount
                    }}
                    issue_mentioned: issues(first: 100, filterBy: {{mentioned: "{AUTHOR}"}}) {{
                      totalCount
                    }}
                  }}
                  pullR: search(query: "repo:{REPO_OWNER}/{REPO_NAME} type:pr author:{AUTHOR}", type: ISSUE, first: 100) {{
                    issueCount
                  }}
                  rateLimit {{
                    cost
                    limit
                    remaining
                    used
                    resetAt
                  }}
                }}
                """

                response_main = requests.post('https://api.github.com/graphql', headers=headers,
                                              data=json.dumps({"query": query_main}))
                result = response_main.json()
                commit_tot = result['data']['repository']['commit']['history']['totalCount']
                issue_assigned = result['data']['repository']['issue_assigned']['totalCount']
                issue_created = result['data']['repository']['issue_created']['totalCount']
                issue_mentioned = result['data']['repository']['issue_mentioned']['totalCount']

                headers = {
                    'Authorization': f'token {GITHUB_TOKEN}',
                    'Accept': 'application/vnd.github.v3+json'
                }

                # Fetch issue comments by the author
                issue_comments_url = f"https://api.github.com/search/issues?q=repo:{REPO_OWNER}/{REPO_NAME}+type:issue+commenter:{AUTHOR}"
                issue_comments_response = requests.get(issue_comments_url, headers=headers)
                issue_comments = issue_comments_response.json()
                tot_issue_comments = issue_comments.get('total_count', 0)

                # Fetch pull request comments by the author
                pr_comments_url = f"https://api.github.com/search/issues?q=repo:{REPO_OWNER}/{REPO_NAME}+type:pr+commenter:{AUTHOR}"
                pr_comments_response = requests.get(pr_comments_url, headers=headers)
                pr_comments = pr_comments_response.json()
                tot_pr_comments = pr_comments.get('total_count', 0)

                # Fetch all commit comments
                commit_comments_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/comments"
                commit_comments_response = requests.get(commit_comments_url, headers=headers)
                commit_comments = commit_comments_response.json()

                # Filter commit comments by the author
                author_commit_comments = [comment for comment in commit_comments if
                                          comment.get('user', {}).get('login') == AUTHOR]
                tot_commit_comments = len(author_commit_comments)

                if not os.path.exists(f"mining_results/{file.replace('.sql', '.csv')}"):
                    with open(f"mining_results/{file.replace('.sql', '.csv')}", "w") as f:
                        f.write(
                            "project_id,owner,repo,git_user_id,login,name,email,commit_tot,issue_assigned,issue_created,issue_mentioned,tot_issue_comments,tot_pr_comments,tot_commit_comments\n")

                with open(f"mining_results/{file.replace('.sql', '.csv')}", "a") as f:
                    f.write(
                        f"{row['project_id']},{REPO_OWNER},{REPO_NAME},{USER_IDS},{AUTHOR},{AUTHOR_NME},{AUTHOR_EMAIL},{commit_tot},{issue_assigned},{issue_created},{issue_mentioned},{tot_issue_comments},{tot_pr_comments},{tot_commit_comments}\n")

                processed_users.add(user_project_key)
                save_processed_user(processed_users_file, user_project_key)
                rate_limit = result['data']['rateLimit']['remaining']
                if rate_limit == 0:
                    reset_time = str_to_datetime(result['data']['rateLimit']['resetAt'])
                    reset_time = utc_to_local(reset_time)
                    wait_until_time_passed(utc_to_local(reset_time))

            except Exception as e:
                content = f'Subject: USER MINING\n\nENCOUNTERED THE FOLLOWING ERROR:\n{e}\n{traceback.format_exc()}\n\t- Analyzing dbs: {file},\n\t- Project: {REPO_NAME},\n\t- Owner: {REPO_OWNER},\n\t- UserName: {AUTHOR_NME},\n\t- UserEmail: {AUTHOR_EMAIL}'
                send_email_notification(content)

content = f"Subject: USER MINING\n\nTHE PROCESS IS COMPLETED:\n\t- YUPPIE YEAH"
send_email_notification(content)
