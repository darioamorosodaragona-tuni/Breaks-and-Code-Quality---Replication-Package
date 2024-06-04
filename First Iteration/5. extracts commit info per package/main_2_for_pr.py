import os
import sqlite3

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm


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
        df = pd.read_csv(f'../unique_commits_per_package_pr/{id}_unique_commit_ids.csv')
    except:
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

        # Construct the SQL query dynamically
        query = f"""
            SELECT *
            FROM git_commit
            WHERE {'id =' if len(commit_ids) == 1 else 'id IN'} ({', '.join(str(commit_id) for commit_id in commit_ids)}) and project_id = {projectId}
        """


        # Execute the query and fetch the results into a Pandas DataFrame
        commits_df = pd.read_sql_query(query, conn)

        if commits_df['commit_date'].isna().sum() > 0:
            with open(f'../errors.csv', 'a') as f:
                f.write(query + '\n')

        commits_df = commits_df[commits_df['commit_date'].notna()]

        if commits_df.empty:
            continue

        commits_df['package'] = package

        commits_df['dates'] = pd.to_datetime(commits_df['commit_date'], utc=True)

        result_df = commits_df

        result_df.drop(columns=['msg', 'commit_date'], inplace=True)
        result_df.to_csv(f'../commits_per_package_pr/db_{id}_{projectId}_{package_id}_commits.csv', index=False)
        package_ids += f'{package},{id},{projectId},{package_id}\n'
        package_id += 1



    with open(f'../package_ids_pr.csv', 'a') as f:
        f.write(package_ids)

    # Close the connection
    conn.close()
