import os
import pandas as pd

# Step 1: Load CSV Files
os.makedirs('cluster_results', exist_ok=True)
files = os.listdir('mining_results')
files = [f for f in files if f.endswith('.csv')]

# Step 2: Concatenate DataFrames
df_result = pd.DataFrame()
resplit = {}

columns_to_sum = [
    'commit_tot', 'issue_assigned', 'issue_created',
    'issue_mentioned', 'tot_issue_comments', 'tot_pr_comments', 'tot_commit_comments'
]

# Calculate the sum of the specified columns and assign it to a new column 'total_sum'


for file in files:
    df = pd.read_csv('mining_results/' + file)
    df['total_sum'] = df[columns_to_sum].sum(axis=1)
    df_result = pd.concat([df_result, df])
    id = file.split('db-split')[1].replace('.csv', '')
    project_ids = df['project_id'].unique()
    resplit[id] = project_ids

# Step 3: Calculate Quintiles and Classify Developers within Each Repo
def classify_developer(tf, quintiles):
    if tf <= quintiles[0.2]:
        return 'super inactive'
    elif tf <= quintiles[0.4]:
        return 'inactive'
    elif tf <= quintiles[0.6]:
        return 'average'
    elif tf <= quintiles[0.8]:
        return 'active'
    else:
        return 'super active'

df_avg = df_result.copy()
df_avg['class'] = None

repos = df_avg['repo'].unique()
for repo in repos:
    repo_data = df_avg[df_avg['repo'] == repo]
    quintiles = repo_data['commit_tot'].quantile([0.2, 0.4, 0.6, 0.8, 1.0])
    df_avg.loc[df_avg['repo'] == repo, 'class'] = repo_data['commit_tot'].apply(lambda tf: classify_developer(tf, quintiles))

# Step 4: Save Full DataFrame with Classifications
df_avg.to_csv('cluster_results/result_all_together.csv', index=False)

# Step 5: Save Separate DataFrames for Each Project Split
for key, project_ids in resplit.items():
    df_split = df_avg[df_avg['project_id'].isin(project_ids)]
    df_split.to_csv(f'cluster_results/db-split{key}clusters.csv', index=False)
