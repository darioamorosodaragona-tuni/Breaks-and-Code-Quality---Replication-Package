import os

import pandas as pd
import tqdm

prs = "../commits_per_package_pr"
commits = '../commits_per_package'
match = pd.read_csv('matching_files.csv')

os.makedirs("../commits_per_package_concat", exist_ok=True)

for index, row in tqdm.tqdm(match.iterrows()):
    db = row['db']
    id_project = row['project_id']
    pkg_id = row['package_id']
    pkg_id_pr = row['package_id_pr']

    data_pr = pd.read_csv(os.path.join(prs, f"db_{db}_{id_project}_{pkg_id_pr}_commits.csv"))
    data_pr["pr"] = True
    data = pd.read_csv(os.path.join(commits, f"db_{db}_{id_project}_{pkg_id}_commits.csv"))
    data["pr"] = False

    data = pd.concat([data, data_pr])
    data.to_csv(os.path.join("../commits_per_package_concat", f"db_{db}_{id_project}_{pkg_id}_{pkg_id_pr}_commits.csv"), index=False)



