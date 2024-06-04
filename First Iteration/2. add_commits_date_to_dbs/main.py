import os
import sqlite3
import pandas as pd
import tqdm

# Connect to SQLite database

dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]

# dbs = dbs1 + dbs2
dbs = dbs2

dates_file_pj_id = os.listdir('input/dates')
dates_file_pj_id = [x.split('.')[0] for x in dates_file_pj_id]

for db in tqdm.tqdm(dbs, desc='dbs', leave=True, position=0):
    conn = sqlite3.connect(f'{db}')
    cursor = conn.cursor()

    # Access the commits table
    table_name = 'git_commit'

    # Load data from CSV into a Pandas DataFrame
    project_ids = pd.read_sql_query(f"SELECT DISTINCT project_id FROM git_commit", conn)

    for project_id in tqdm.tqdm(project_ids['project_id'], desc='projects', leave=False, position=1):
        csv_file_path = f'input/dates/{project_id}.csv'
        csv_data = pd.read_csv(csv_file_path, delimiter=',', header=None, names=['hash', 'data'])

        cursor.execute(f"PRAGMA table_info({table_name})")
        table_info = cursor.fetchall()
        columns = [column[1] for column in table_info]
        data_column_exists = 'commit_date' in columns

        if not data_column_exists:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN commit_date TEXT")

        # Iterate through each row in the DataFrame and update the database
        for index, row in csv_data.iterrows():
            hash_value = row['hash']
            data_value = row['data']

            # Execute the SQL update statement
            update_query = f"UPDATE {table_name} SET commit_date = ? WHERE hash = ?"
            cursor.execute(update_query, (data_value, hash_value))

        # Commit the changes and close the connection
        conn.commit()
    conn.close()