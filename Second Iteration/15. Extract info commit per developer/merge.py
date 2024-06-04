import os

import pandas as pd
import tqdm


prs = "../commits_per_developer_pr"
commits = '../commits_per_developer'
os.makedirs("../commits_per_developer_concat", exist_ok=True)

files = os.listdir("../commits_per_developer")

for file in files:
    split = file.split('_')
    db = split[1]
    id_project = split[2]
    developer = split[3]

    for index, row in pd.read_csv(os.path.join(commits, file)).iterrows():
        try:
            data_pr = pd.read_csv(os.path.join(prs, f"db_{db}_{id_project}_{developer}_commits.csv"))
            data_pr["pr"] = True
            data = pd.read_csv(os.path.join(commits, f"db_{db}_{id_project}_{developer}_commits.csv"))
            data["pr"] = False
            data = pd.concat([data, data_pr])
            data.to_csv(os.path.join("../commits_per_developer_concat", f"db_{db}_{id_project}_{developer}_commits.csv"), index=False)
        except:
            data = pd.read_csv(os.path.join(commits, f"db_{db}_{id_project}_{developer}_commits.csv"))
            data["pr"] = False
            data.to_csv(os.path.join("../commits_per_developer_concat", f"db_{db}_{id_project}_{developer}_commits.csv"), index=False)





