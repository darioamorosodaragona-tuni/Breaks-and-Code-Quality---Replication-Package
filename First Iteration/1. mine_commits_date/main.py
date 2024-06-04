import os
import sqlite3
import pandas as pd
import pydriller
import tqdm

dbs1 = os.listdir('../dbs')
dbs1 = [x for x in dbs1 if x.startswith('db-split')]
dbs1 = [f'../dbs/{x}' for x in dbs1 if x.endswith('.sql')]

dbs2 = os.listdir('../pull_request_db')
dbs2 = [x for x in dbs2 if x.startswith('db-pl')]
dbs2 = [f'../pull_request_db/{x}' for x in dbs2 if x.endswith('.sqlite')]

# dbs = dbs1 + dbs2
dbs = dbs2

db_completed = pd.read_csv('db_completed.csv')
db_completed = db_completed['db'].tolist()

dbs = [x for x in dbs if x not in db_completed]
# to_analyze = [ "db-split6.sql"]
# dbs = [x for x in dbs if x in to_analyze]

for db in tqdm.tqdm(dbs, desc='dbs', leave=True, position=0):

    # Connect to SQLite database

    conn = (sqlite3.connect(f"{db}"))


    # conn = (sqlite3.connect(f"../dbs/{db}"))
    #
    # Access a specific table (replace 'your_table_name' with the actual table name)
    git_files = 'git_files'
    git_commits = 'git_commit'

    # Read data from the table into a Pandas DataFrame
    query = f"SELECT hash, github_url, projects.id FROM {git_commits}, projects WHERE projects.id = {git_commits}.project_id"
    df = pd.read_sql_query(query, conn)
    # Close the connection
    conn.close()

    completed = os.listdir('dates')
    completed = [int(x.split('.')[0]) for x in completed]
    # Print the DataFrame
    for name, group in tqdm.tqdm(df.groupby('github_url'), desc='repos', leave=False, position=1):
        # print(name)
        # print(group.hash)
        dates = {name: {}}

        # if group.id.iloc[0] in completed:
        #     continue

        for commit in pydriller.Repository(name, only_commits=group.hash).traverse_commits():
            dates[name][commit.hash] = commit.committer_date

        pandas_dates = pd.DataFrame.from_dict(dates)
        pandas_dates.to_csv(f'dates/{group.id.iloc[0]}.csv')

    db_completed.append(db)
    pd.DataFrame(db_completed, columns=['db']).to_csv('db_completed.csv', index=False)





