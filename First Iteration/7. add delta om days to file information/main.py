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
dbs = dbs1 + dbs2

os.makedirs('../files_per_package_with_sonar', exist_ok=True)
package_id = 0
for db in tqdm(dbs, position=0, leave=True, desc="Processing databases"):
    # Connect to SQLite database
    try:
        db_id = db.replace(".sql", "").split("db-split")[1]
    except:
        db_id = db.replace(".sqlite", "").split("db-pl-split")[1]

    files = os.listdir('../commits_per_package_concat_with_delta')
    files = [x for x in files if x.endswith('.csv')]
    files = [x for x in files if x.startswith(f'db_{db_id}')]

    for file in tqdm(files, position=1, leave=False, desc="Processing packages"):


        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(f'../commits_per_package_concat_with_delta/{file}')

        # Iterate over rows in the DataFrame and construct SQL queries dynamically
        file_df = pd.DataFrame()
        for index, row in df.iterrows():

            commit_id = row['id']
            package = row['package']
            delta = row['delta']
            prq = row['pr']
            try:
                commit_delta_date = row['commits_delta_date']
            except:
                commit_delta_date = None

            if commit_id == 'issue':
                continue

            # Construct the SQL query dynamically
            query = f"""
                SELECT *
                FROM git_files
                WHERE git_commit_id = {commit_id} and package = '{package}'
            """

            query2 = f'''
                SELECT sonar_measures.id
                FROM git_commit
                INNER JOIN sonar_revisions ON git_commit.hash = sonar_revisions.revision
                INNER JOIN sonar_measures ON sonar_revisions.id = sonar_measures.revision_id
                WHERE git_commit.id = {commit_id}
            '''

            sonar = {'id': []}

            # Execute the query and fetch the results into a Pandas DataFrame
            if row['pr']:
                db_to_connect = ""
                for d in dbs2:
                    if db_id in d:
                        db_to_connect = d
                        break
                conn = sqlite3.connect(db_to_connect)
                try:
                    single_line = pd.read_sql_query(query, conn)
                except:
                    pass
                conn.close()
            else:
                conn = sqlite3.connect(db)
                single_line = pd.read_sql_query(query, conn)
                try:
                    sonar = pd.read_sql_query(query2, conn)
                except:
                    with open('errors.csv', 'a') as f:
                        f.write(f"{file},{db_id},{commit_id},{package},{prq}\n")
                conn.close()

            single_line['action_delta_date'] = delta
            single_line['commits_delta_date'] = commit_delta_date
            single_line['pr'] = prq
            single_line['sonar_ids'] = None
            if len(sonar['id']) > 0:
                single_line['sonar_ids'] = ','.join(sonar['id'].T.astype(str).tolist())
            file_df = pd.concat([file_df, single_line])

        if os.path.exists(f'../files_per_package_with_sonar/{file}'):
            file_df.to_csv(f'../files_per_package_with_sonar/{file}', mode="a", index=False, header=False)
        else:
            file_df.to_csv(f'../files_per_package_with_sonar/{file}', mode="a", index=False)
