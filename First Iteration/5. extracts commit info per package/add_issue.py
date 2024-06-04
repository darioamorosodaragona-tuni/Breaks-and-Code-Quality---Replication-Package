import math
import os
import sqlite3

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm

os.makedirs('../commits_per_package_pr', exist_ok=True)

dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]

# dbs = dbs1 + dbs2
dbs = dbs2

# dbs = os.listdir('../dbs')
# dbs = [x for x in dbs if x.endswith('.sql')]
# dbs = [x for x in dbs if x.startswith('db-split')]
# to_analyze = ["db-split6.sql"]
# dbs = [x for x in dbs if x in to_analyze]
# package_id = 0
package_ids_header = 'package,db,project_id,package_id\n'
package_ids = ''


# package_ids_max = os.listdir('../commits_per_package')
# package_ids_max = [int(x.split('_')[3]) for x in package_ids_max]
# package_ids_max = max(package_ids_max)
package_id = 0

with open(f'../package_ids_pr.csv', 'w') as f:
    f.write(package_ids_header)


for db in tqdm(dbs, leave=True, desc="dbs", position=0):

    with open(f'../errors.csv', 'w') as f:
        f.write('')

    # Connect to SQLite database
    conn = sqlite3.connect(db)

    id = db.replace(".sqlite", "").split("db-pl-split")[1]


    # Read the CSV file into a Pandas DataFrame
    df = pd.DataFrame()
    try:
        df = pd.read_csv(f'../unique_commits_per_package_pr_with_pr_id/{id}_unique_commits_ids.csv')
    except Exception as e:
        print(e)
        continue

    # Iterate over rows in the DataFrame and construct SQL queries dynamically
    for index, row in tqdm(df.iterrows(), position=1, leave=False, desc="package"):

        # if db not in to_analyze:
        #     package_id += 1
        #     continue

        package = row['package']
        commit_ids = row['unique_commit_ids'].split(',')
        commit_ids = [int(commit_id) for commit_id in commit_ids]  # Convert commit_ids to integers
        projectId = row['project_id']

        # analyze = False
        # if id == '4':
        #     if projectId == 13:
        #         if package_id == 3415:
        #              analyze = True
        #
        # if not analyze:
        #     package_id += 1
        #     continue

        # debug = """
        #     SELECT *
        #     FROM git_commit
        #     WHERE id IN (1495, 1568) and project_id = 30
        #     """
        #
        # query = debug

        # Construct the SQL query dynamically
        query = f"""
            SELECT *
            FROM git_commit
            WHERE {'id =' if len(commit_ids) == 1 else 'id IN'} ({', '.join(str(commit_id) for commit_id in commit_ids)}) and project_id = {projectId}
        """

        query2 = f"""
            SELECT *
            FROM gh_issues
            WHERE project_id == {projectId}
        """

        issues_association = pd.read_csv(f"../11. associate_pr_to_issues/from_issue/{id}_prs_issues.csv")
        issues_association = pd.concat([issues_association, pd.read_csv(f"../11. associate_pr_to_issues/from_pr/{id}_prs_issues.csv"), pd.read_csv(f"../11. associate_pr_to_issues/from_pr/{id}_prs_closing_issues.csv")])
        issues_association = issues_association[issues_association['project_id'] == projectId]


        # pull_request_number = str(row['prq_number']).split(',')
        if math.isnan(row['prq_number']):
            pull_request_number = []
        else:
            pull_request_number = [int(float(num)) for num in str(row['prq_number']).split(',') if num.replace('.', '').isdigit()]
        issues_association = issues_association[issues_association['prs_id'].isin(pull_request_number)]


        # query3 = f"""
        #     SELECT *
        #     FROM gh_pull
        #     WHERE project_id == {projectId}
        # """

        # Execute the query and fetch the results into a Pandas DataFrame
        conn = sqlite3.connect(db)
        commits_df = pd.read_sql_query(query, conn)
        conn.close()

        if commits_df['commit_date'].isna().sum() > 0:
            with open(f'../errors.csv', 'a') as f:
                f.write(query + '\n')

        commits_df = commits_df[commits_df['commit_date'].notna()]

        if commits_df.empty:
            continue

        commits_df['package'] = package

        # pull = pd.read_sql_query(query3, conn)
        # pull = pull[['project_id', 'created_at', 'closed_at', 'updated_at', 'merged_at']]
        # pull['created_at'] = pd.to_datetime(pull['created_at'], utc=True)
        # pull['closed_at'] = pd.to_datetime(pull['closed_at'], utc=True)
        # pull['updated_at'] = pd.to_datetime(pull['updated_at'], utc=True)
        # pull['merged_at'] = pd.to_datetime(pull['merged_at'], utc=True)
        #
        # pull = pull[pull['created_at'].notna()]
        # pull = pull[pull['closed_at'].notna()]
        # pull = pull[pull['updated_at'].notna()]
        #
        # pull.rename(columns={'merged_at': 'pull_merged_at', 'created_at': 'pull_created_at', 'closed_at': 'pull_closed_at','updated_at': 'pull_updated_at'}, inplace=True)

        db_to_connect = ""
        for d in dbs1:
            if id in d:
                db_to_connect = d
                break
        conn = sqlite3.connect(db_to_connect)
        issue_df = pd.read_sql_query(query2, conn)
        conn.close()

        issue_df = issue_df[issue_df['issue_number'].isin(issues_association['issues_id'])]
        if not issue_df.empty:
            with open(f'../not_empty.csv', 'a') as f:
                f.write(f"db_{id}_{projectId}_{package_id}_commits.csv" + '\n')

        issue_df = issue_df[['project_id', 'created_at', 'closed_at', 'updated_at']]
        issue_df['created_at'] = pd.to_datetime(issue_df['created_at'], utc=True)
        issue_df['closed_at'] = pd.to_datetime(issue_df['closed_at'], utc=True)

        issue_df = issue_df[issue_df['created_at'].notna()]
        issue_df = issue_df[issue_df['closed_at'].notna()]
        issue_df = issue_df[issue_df['updated_at'].notna()]

        commits_df = commits_df[commits_df['commit_date'].notna()]

        if commits_df.empty:
            continue

        commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'], utc=True)

        issue_df.drop(columns=['project_id'], inplace=True)
        # pull.drop(columns=['project_id'], inplace=True)

        # pull = pull[pull['pull_created_at'] >= commits_df['commit_date'].sort_values().iloc[0]]
        # pull = pull[pull['pull_created_at'] <= commits_df['commit_date'].sort_values().iloc[-1]]

        issue_df = issue_df[issue_df['created_at'] >= commits_df['commit_date'].sort_values().iloc[0]]
        issue_df = issue_df[issue_df['created_at'] <= commits_df['commit_date'].sort_values().iloc[-1]]

        # result_df = pd.concat([commits_df, issue_df, pull], axis=1)
        result_df = pd.concat([commits_df, issue_df], axis=1)

        dates_df = pd.DataFrame({
            'dates': pd.concat(
                # [result_df[col].dropna() for col in
                #  ['commit_date', 'created_at', 'closed_at', 'updated_at', 'pull_created_at', 'pull_closed_at',
                #   'pull_updated_at', 'pull_merged_at']]).values

                [result_df[col].dropna() for col in ['commit_date', 'created_at', 'closed_at', 'updated_at']]).values
        })

        # Drop 'created_at', 'closed_at', and 'updated_at' columns
        # result_df.drop(columns=['commit_date', 'created_at', 'closed_at', 'updated_at', 'pull_created_at', 'pull_closed_at', 'pull_updated_at', 'pull_merged_at'], inplace=True)
        result_df.drop(columns=['commit_date', 'created_at', 'closed_at', 'updated_at'], inplace=True)

        result_df = result_df[result_df['id'].notna()]

        # Reset the index of dates_df to match df
        dates_df.reset_index(drop=True, inplace=True)

        # Concatenate the original dataframe with dates_df
        result_df = pd.concat([result_df, dates_df], axis=1)

        # OLD CODE DONT' USED
        # result_df['dates'].fillna(pd.Timestamp('issue'), inplace=True)
        result_df.fillna('issue', inplace=True)
        # OLD CODE DONT' USED
        # merged_df = pd.merge(commits_df, issue_df, on='project_id',  how='left')
        # commits_df = commits_df.merge(issue_df, on='project_id', how='left')
        result_df.drop(columns=['msg'], inplace=True)
        result_df.to_csv(f'../commits_per_package_pr/db_{id}_{projectId}_{package_id}_commits.csv', index=False)
        package_ids += f'{package},{id},{projectId},{package_id}\n'
        package_id += 1

    with open(f'../package_ids_pr.csv', 'a') as f:
        f.write(package_ids)

