import os
import sqlite3
import pandas as pd
import tqdm

# Connect to SQLite database
# conn = sqlite3.connect('../dbs/db-split0.sql')
# cursor = conn.cursor()



dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]

# dbs = dbs1 + dbs2
dbs = dbs2

# Fetch data into a Pandas DataFrame
table_name = "git_files"
query = f"SELECT id, new_path, old_path FROM {table_name}"
new_columns = 'package'

# to_analyze = ["db-split6.sql"]
# dbs = [x for x in dbs if x in to_analyze]

for db in tqdm.tqdm(dbs, desc='dbs', leave=True, position=0):
    conn = sqlite3.connect(f'{db}')
    cursor = conn.cursor()
    df = pd.read_sql_query(query, conn)

    # Calculate the new_path_modified
    df['new_path'].fillna('', inplace=True)
    df['new_path_modified'] = df.apply(lambda row: row['new_path'].rsplit('/', 1)[0] if row['new_path'] else row['old_path'].rsplit('/', 1)[0], axis=1)

    # Update the SQLite database with the modified values
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    columns = [column[1] for column in table_info]
    data_column_exists = new_columns in columns

    if not data_column_exists:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {new_columns} TEXT")

    for index, row in df.iterrows():
        cursor = conn.cursor()
        update_query = f"UPDATE {table_name} SET {new_columns} = ? WHERE id = ?"
        cursor.execute(update_query, (row['new_path_modified'], row['id']))
        conn.commit()

    # Close the connection
    conn.close()
