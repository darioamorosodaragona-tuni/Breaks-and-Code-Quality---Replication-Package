import math
import os
import sqlite3

import pandas as pd
from tqdm import tqdm


def update_column_A(row, commit_id, project_id, pull_request_number):
    if commit_id in row['unique_commit_ids'].split(','):
        if project_id == row['project_id']:
            return pull_request_number + ',' + str(row['prq_number']) if pd.notna(
                row['prq_number']) else pull_request_number
    else:
        return row['prq_number']

dir = "../../Data"

commits_per_pr = f"{dir}/commits_per_pr"
commits_per_package = f"../unique_commits_per_developer_pr"
os.makedirs("../unique_commits_per_developer_pr_with_pr_id", exist_ok=True)

for file in tqdm(os.listdir(commits_per_pr), position=0, leave=True, desc="Processing commits per PR"):
    if file.endswith('.sql.csv'):
        df = pd.read_csv(os.path.join(commits_per_pr, file))
        df = df[pd.notna(df['id'])]

        for index, row in tqdm(df.iterrows(), position=1, leave=False, desc="Processing rows"):

            project_id = row['project_id']
            commit_id = str(int(row['id']))
            db = file.replace(".sql.csv", "").split("db-split")[1]
            pull_request_number = row['pullrequest']

            # package_id = file.split("_")[1]
            df_pr = pd.read_csv(os.path.join(commits_per_package, f"{db}_unique_commit_ids.csv"))
            if 'prq_number' not in df_pr.columns:
                df_pr['prq_number'] = math.nan

            # Apply the function to update values in column 'A'
            df_pr['prq_number'] = df_pr.apply(
                lambda row: update_column_A(row, commit_id, project_id, pull_request_number), axis=1)

            # da qui
            df_pr.to_csv(f"../unique_commits_per_developer_pr_with_pr_id/{db}_unique_commits_ids.csv", index=False)
