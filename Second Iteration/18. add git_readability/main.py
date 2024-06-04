import os
import sqlite3
import warnings

import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm

dir = "../../Data"

dbs1 = os.listdir(f'{dir}/dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'{dir}/dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir(f'{dir}/pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'{dir}/pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]
os.makedirs('../files_per_developer_with_metric_and_sonar', exist_ok=True)
dbs = dbs1 + dbs2

# DEBUG
# dbs = dbs1
# dbs = [x for x in dbs if "db-split4" in x]

for db in tqdm(dbs, position=0, leave=True, desc="Processing databases"):
    # Connect to SQLite database
    # conn = sqlite3.connect(db)

    try:
        db_id = db.replace(".sql", "").split("db-split")[1]
    except:
        db_id = db.replace(".sqlite", "").split("db-pl-split")[1]

    files = os.listdir('../files_per_developer_with_sonar')
    files = [x for x in files if x.endswith('.csv')]
    files = [x for x in files if x.startswith(f'db_{db_id}')]

    for file in tqdm(files, position=1, leave=False, desc="Processing Developers"):

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(f'../files_per_developer_with_sonar/{file}')

        # if file != "db_0_1_11953_2302_commits.csv":
        #     continue

        if df.empty:
            continue
        df["git_file_id"] = df["id"]

        # Iterate over rows in the DataFrame and construct SQL queries dynamically
        file_df = pd.DataFrame()
        for index, row in df.iterrows():
            file_id = row["id"]
            sonar_measure_id = None if pd.isna(row['sonar_ids']) else str(row['sonar_ids'])
            if sonar_measure_id:
                if "," in sonar_measure_id:
                    sonar_measure_id = [int(num) for num in sonar_measure_id.split(',')]
                else:
                    sonar_measure_id = [int(float(sonar_measure_id))]


            # Construct the SQL query dynamically
            query = f"""
                SELECT *
                FROM git_readability
                WHERE git_readability.git_file_id = '{file_id}'
            """

            if sonar_measure_id:
                sonar_measure_id_str = [str(x) for x in sonar_measure_id]

                query2 = f"""
                SELECT *
                FROM sonar_measures
                WHERE {'id =' if len(sonar_measure_id) == 1 else 'id IN'} ({', '.join(sonar_measure_id_str)}) 
            """


            # Execute the query and fetch the results into a Pandas DataFrame
            single_line = pd.DataFrame()
            sonar = pd.DataFrame()
            if not row['pr']:
                db_to_connect = ""
                for d in dbs1:
                    if db_id in d:
                        db_to_connect = d
                        break
                conn = sqlite3.connect(db_to_connect)
                try:
                    single_line = pd.read_sql_query(query, conn)
                    if sonar_measure_id:
                        try:
                            sonar = pd.read_sql_query(query2, conn)
                        except:
                            pass
                except:
                    pass
                conn.close()
            else:
                conn = sqlite3.connect(db)
                single_line = pd.read_sql_query(query, conn)
                conn.close()

            if not sonar.empty:
                sonar.rename(columns={"id": "sonar_measures_id"}, inplace=True)
                single_line = single_line.join(sonar, how="outer")

            file_df = pd.concat([file_df, single_line])

        try:
            df = pd.merge(df, file_df, on=['git_file_id'], how='left', suffixes=('_git', '_sonar'))
        except:
            pass
        df.drop(columns=['id_git'], inplace=True)
        df.drop(columns=['id_sonar'], inplace=True)
        df.drop(columns=['worker_id_git'], inplace=True)
        df.drop(columns=['worker_id_sonar'], inplace=True)
        # df.drop(columns=['git_file_id'], inplace=True)



        if os.path.exists(f"../files_per_developer_with_metric_and_sonar/{file}"):
            df.to_csv(f'../files_per_developer_with_metric_and_sonar/{file}', mode="a", header=False, index=False)
        else:
            df.to_csv(f'../files_per_developer_with_metric_and_sonar/{file}', index=False)
