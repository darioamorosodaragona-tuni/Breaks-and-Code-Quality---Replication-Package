import os
import sqlite3

import pandas as pd
from tqdm import tqdm

commits_per_pr = "../commits_per_pr"

for file in tqdm(os.listdir(commits_per_pr)):
    if file.endswith('.sql.csv') and 'pagination' not in file:
        df = pd.read_csv(os.path.join(commits_per_pr, file), on_bad_lines = 'skip')
        df['id'] = None
        for index, row in tqdm(df.iterrows()):
            # package = row['package']
            project_id = row['project_id']
            commit_hash = row['commit']
            db = file.split("_")[0]
            db_id = file.replace(".sql.csv", "").split("db-split")[1]
            database = f"../pull_request_db/db-pl-split{db_id}.sqlite"
            conn = sqlite3.connect(database)

            query = f"SELECT id FROM git_commit WHERE hash = '{commit_hash}' AND project_id = {project_id}"

            single_line = pd.read_sql_query(query, conn)

            conn.close()

            if len(single_line) == 0:
                continue
            df.at[index, 'id'] = single_line['id'].iloc[0]

        df.to_csv(f"../commits_per_pr/{file}", index=False)
