import os
import sqlite3
import time
import datetime

import pandas as pd
import pytz
import requests
import tqdm


# Function to execute GitHub GraphQL query
def execute_github_query(owner, repo, issue_number):
    DEBUG = True


    if DEBUG:
        owner = "apache"
        repo = "jmeter"
        issue_number = 6114


    referred_pull_request = []
    tracked_issues = []

    query = '''query {
  repository(owner: "%s", name: "%s") {
    issue(number: %d) {
      trackedIssues(first:100){
        edges{
          node{
            number
          }
        }
      }
      

      timelineItems(first: 100) {
        nodes {
          ... on CrossReferencedEvent {
            source {
              ... on PullRequest {
                number
              }
            }
          }
        }
      }
    }
  }
}''' % (owner, repo, issue_number)

    #Todo: change the github token
    headers = {"Authorization": "token [GIT_TOKEN]"}
    response = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
    json_data = response.json()

    rate_limit = int(response.headers['X-RateLimit-Remaining'])
    reset_time_str = response.headers['X-RateLimit-Reset']
    reset_time = datetime.datetime.utcfromtimestamp(int(reset_time_str))
    current_time = datetime.datetime.utcnow()

    # Check rate limit
    if rate_limit == 0:
        wait_time = (reset_time - current_time).total_seconds()
        # print(f"Rate limit exceeded. Waiting for {wait_time} seconds until reset.")
        for _ in tqdm.tqdm(range(int(wait_time + 10)), desc='waiting', position=3, leave=False):
            time.sleep(1)
        # time.sleep(wait_time + 10)  # Add extra 10 seconds buffer
        execute_github_query(owner, repo, issue_number)
    else:
        # Process data
        if 'errors' in json_data:
            print("Error:", json_data['errors'])
        else:
            nodes = json_data['data']['repository']['issue']['timelineItems']['nodes']
            for node in tqdm.tqdm(nodes, desc='pull_requests', position=2, leave=False):
                if 'source' in node and 'number' in node['source']:
                    pull_request = node['source']['number']
                    referred_pull_request.append(pull_request)
            edges = json_data['data']['repository']['issue']['trackedIssues']['edges']
            for edge in tqdm.tqdm(edges, desc='tracked_issues', position=2, leave=False):
                pull_request = edge['node']['number']
                tracked_issues.append(pull_request)
    return referred_pull_request, tracked_issues


# Call this function for each row in your dataset
def process_dataset(db):
    db_id = db.replace(".sql", "").split('../dbs/db-split')[1]
    already_mined = f'./from_issue/{db_id}_mined_prq.csv'
    referred_pull_request = f'./from_issue/{db_id}_prs_issues.csv'
    tracked_issues = f'./from_issue/{db_id}_tracked_issues.csv'

    mined = []

    if os.path.exists(already_mined):
        try:
            mined = pd.read_csv(already_mined)['id'].tolist()
        except:
            pass

    if not os.path.exists(referred_pull_request):
        with open(referred_pull_request, 'w') as file:
            file.write('id,project_id,prs_id,issues_id\n')

    if not os.path.exists(tracked_issues):
        with open(tracked_issues, 'w') as file:
            file.write('id,project_id,issue_id,issues_tracked_id\n')

    # Connect to SQLite database
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # Fetch data from gh_issues table
    cursor.execute("SELECT id, project_id, html_url, issue_number FROM gh_issues")
    rows = cursor.fetchall()

    # Iterate through each row and execute GitHub query
    for row in tqdm.tqdm(rows, desc='rows', leave=False, position=1):
        _id, project_id, html_url, issue_number = row

        if _id in mined:
            continue
        # Extract owner and repo from html_url

        # if 'pull' in html_url:
        #     continue

        owner_repo = html_url.split('/')[3:5]
        owner, repo = owner_repo[0], owner_repo[1]
        # print("Executing query for issue number:", issue_number)

        try:
            referred_pull_request_result, tracked_issues_result = execute_github_query(owner, repo, issue_number)
            mined.append(_id)
        finally:
            # with open(already_mined, 'w') as file:
            #     file.write('prs_id\n')
            #     for pull_number in mined:
            #         file.write(f"{pull_number}\n")
            data = pd.DataFrame(mined, columns=['id'])
            data.to_csv(already_mined, index=False)

        if len(referred_pull_request_result) > 0:
            data = pd.DataFrame({'id': [_id] * len(referred_pull_request_result),'project_id': [project_id] * len(referred_pull_request_result),'prs_id': [issue_number] * len(referred_pull_request_result), 'issues_id': referred_pull_request_result})
            data.to_csv(referred_pull_request, mode='a', index=False, header=False)
            # with open(referred_issues, 'a') as file:
            #     for issue in referred_issues_result:
            #         file.write(f"{pull_number},{issue}\n")

        if len(tracked_issues_result) > 0:
            data = pd.DataFrame(
                {'id': [_id] * len(tracked_issues_result),'project_id': [project_id] * len(tracked_issues_result), 'prs_id': [issue_number] * len(tracked_issues_result), 'issues_tracked_id': tracked_issues_result})
            data.to_csv(tracked_issues, mode='a', index=False, header=False)

            # with open(closing_issues, 'a') as file:
            #     for issue in closing_issues_result:
            #         file.write(f"{pull_number},{issue}\n")

    # rows = [("owner", "repo", 123), ("owner", "repo", 456)]
    #
    # for row in rows:
    #     owner, repo, issue_number = row
    #     print("Executing query for issue number:", issue_number)
    #     execute_github_query(owner, repo, issue_number)


# Main function
def main():
    #TODO: change the path to point to the dbs folder
    dbs = os.listdir('../dbs')
    dbs = [db for db in dbs if db.endswith('.sql')]
    os.makedirs('./from_issue', exist_ok=True)
    for db in tqdm.tqdm(dbs, desc='dbs', position=0):
        process_dataset(os.path.join('../dbs', db))
        # print("Dataset processed. Waiting for the next batch...")
        # time.sleep(60)  # Wait for 1 minute between batches


if __name__ == "__main__":
    main()
