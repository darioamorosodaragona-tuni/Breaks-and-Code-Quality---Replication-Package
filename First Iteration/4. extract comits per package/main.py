import os
import sqlite3
import pandas as pd


dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]

os.makedirs('../unique_commits_per_package_pr', exist_ok=True)

# dbs = dbs1 + dbs2
dbs = dbs2

# Fetch data into a Pandas DataFrame
# to_analyze = ["db-split6.sql"]
# dbs = [x for x in dbs if x in to_analyze]

git_files = "git_files"
# query = f"SELECT id, package, git_commit_id FROM {table_name}"
query = f"""
    SELECT {git_files}.package, git_commit.project_id, GROUP_CONCAT(DISTINCT git_commit_id) AS unique_commit_ids
    FROM {git_files}, git_commit
    WHERE {git_files}.git_commit_id = git_commit.id
    GROUP BY {git_files}.package, git_commit.project_id
"""


for db in dbs:
    conn = sqlite3.connect(db)
    # cursor = conn.cursor()
    df = pd.read_sql_query(query, conn)
    # db-split0.sql
    try:
        df.to_csv(f'../unique_commits_per_package_pr/{db.replace(".sqlite", "").split("db-pl-split")[1]}_unique_commit_ids.csv', index=False)
    except:
        df.to_csv(f'../unique_commits_per_package/{db.replace(".sql", "").split("db-split")[1]}_unique_commit_ids.csv', index=False)
    conn.close()
