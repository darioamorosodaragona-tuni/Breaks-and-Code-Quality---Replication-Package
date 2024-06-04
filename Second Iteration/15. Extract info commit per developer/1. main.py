import os
import sqlite3

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm

dir = "../../Data"
os.makedirs('../commits_per_developer', exist_ok=True)

dbs1 = os.listdir(f'{dir}/dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'{dir}/dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs = dbs1

for db in tqdm(dbs, leave=True, desc="dbs", position=0):

    with open(f'../errors.csv', 'w') as f:
        f.write('')

    # Connect to SQLite database
    conn = sqlite3.connect(db)
    id = db.replace(".sql", "").split("db-split")[1]

    # Read the CSV file into a Pandas DataFrame
    df = pd.DataFrame()
    try:
        df = pd.read_csv(f'../unique_commits_per_developer/{id}_unique_commit_ids.csv')
    except:
        continue

    # Iterate over rows in the DataFrame and construct SQL queries dynamically
    for index, row in tqdm(df.iterrows(), position=1, leave=False, desc="package"):

        developer = row['git_user_id']
        commit_ids = row['unique_commit_ids'].split(',')
        commit_ids = [int(commit_id) for commit_id in commit_ids]  # Convert commit_ids to integers
        projectId = row['project_id']

        # Construct the SQL query dynamically
        query = f"""
            SELECT *
            FROM git_commit
            WHERE {'id =' if len(commit_ids) == 1 else 'id IN'} ({', '.join(str(commit_id) for commit_id in commit_ids)}) and project_id = {projectId}
        """

        commits_df = pd.read_sql_query(query, conn)

        if commits_df['commit_date'].isna().sum() > 0:
            with open(f'../errors.csv', 'a') as f:
                f.write(query + '\n')

        commits_df = commits_df[commits_df['commit_date'].notna()]

        if commits_df.empty:
            continue

        commits_df['developer'] = developer

        commits_df = commits_df[commits_df['commit_date'].notna()]

        if commits_df.empty:
            continue

        commits_df['dates'] = pd.to_datetime(commits_df['commit_date'], utc=True)

        result_df = commits_df

        result_df.drop(columns=['msg', 'commit_date'], inplace=True)

        result_df.to_csv(f'../commits_per_developer/db_{id}_{projectId}_{developer}_commits.csv', index=False)

    # Close the connection
    conn.close()
