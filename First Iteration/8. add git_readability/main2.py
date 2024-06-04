import os
import sqlite3
import warnings

import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm

def build_query(commit_ids):
    query =  f"""
                  SELECT commit_count,file_count, line_count
                  FROM main.git_oexp
                  WHERE {'git_commit_id =' if len(commit_ids) == 1 else 'id IN'} ({', '.join(commit_ids)}) 
              """
    return query

dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]
os.makedirs('../files_per_package_with_metric_and_sonar', exist_ok=True)
# dbs = dbs1 + dbs2
dbs = dbs2
for db in tqdm(dbs, position=0, leave=True, desc="Processing databases"):
    # Connect to SQLite database
    # conn = sqlite3.connect(db)

    try:
        db_id = db.replace(".sql", "").split("db-split")[1]
    except:
        db_id = db.replace(".sqlite", "").split("db-pl-split")[1]

    files = os.listdir('../files_per_package_with_metric_and_sonar')
    files = [x for x in files if x.endswith('.csv')]
    files = [x for x in files if x.startswith(f'db_{db_id}')]

    for file in tqdm(files, position=1, leave=False, desc="Processing packages"):

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(f'../files_per_package_with_metric_and_sonar/{file}')

        # if file != "db_0_1_11953_2302_commits.csv":
        #     continue

        if df.empty:
            continue

        # Iterate over rows in the DataFrame and construct SQL queries dynamically
        file_df = pd.DataFrame()
        commit_ids = df[df['row'] != True]['git_commit_id'].tolist()
        pr_ids = df[df['row'] == True]['git_commit_id'].tolist()

        query = build_query(commit_ids)


        # Execute the query and fetch the results into a Pandas DataFrame
        results = pd.DataFrame()
        db_to_connect = ""
        for d in dbs1:
            if db_id in d:
                db_to_connect = d
                break
            conn = sqlite3.connect(db_to_connect)
            try:
                results = pd.read_sql_query(query, conn)

            except:
                pass
            conn.close()

        db_to_connect = ""
        for d in dbs2:
            if db_id in d:
                db_to_connect = d
                break
            conn = sqlite3.connect(db_to_connect)
            try:
                results = pd.read_sql_query(query, conn)

            except:
                pass
            conn.close()




        if not results.empty:
            df = pd.merge(df, results, on=['git_commit_id'], how='left')
            sonar.rename(columns={"id": "sonar_measures_id"}, inplace=True)
            single_line = single_line.join(sonar, how="outer")

        file_df = pd.concat([file_df, single_line])

        try:
            df = pd.merge(df, file_df, on=['git_file_id'], how='left')
        except:
            pass
        df.drop(columns=['id_x'], inplace=True)
        df.drop(columns=['id_y'], inplace=True)
        df.drop(columns=['worker_id_x'], inplace=True)
        df.drop(columns=['worker_id_y'], inplace=True)
        # df.drop(columns=['git_file_id'], inplace=True)
        if os.path.exists(f"../files_per_package_with_metric_and_sonar/{file}"):
            df.to_csv(f'../files_per_package_with_metric_and_sonar/{file}', mode="a", header=False, index=False)
        else:
            df.to_csv(f'../files_per_package_with_metric_and_sonar/{file}', index=False)
