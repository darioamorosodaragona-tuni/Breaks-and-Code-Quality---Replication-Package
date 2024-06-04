import os

import pandas as pd

clusters = os.listdir("clusters_with_ids")
clusters = [x for x in clusters if x.endswith('.csv')]
os.makedirs('files_per_clusters', exist_ok=True)
for file in clusters:
    df = pd.read_csv(f'clusters_with_ids/{file}')
    df = df.dropna(subset=['git_user_id'])
    df['git_user_id'] = df['git_user_id'].astype('int')
    for index, row in df.iterrows():
        dev_id = row['git_user_id']
        project_id = row['project_id']
        db_id = file.split('db-split')[1].split('clusters.csv')[0]
        cls = row['class']
        try:
            deltas = pd.read_csv(f'../files_per_developer_with_metric_and_sonar_with_delta/db_{db_id}_{project_id}_{dev_id}_commits.csv')
            if os.path.exists(f'files_per_clusters/{db_id}_{project_id}_{cls}'):
                data = pd.read_csv(f'files_per_clusters/{db_id}_{project_id}_{cls}')
                data = pd.concat([data, deltas])
                data.to_csv(f'files_per_clusters/{db_id}_{project_id}_{cls}', index=False)
            else:
                deltas.to_csv(f'files_per_clusters/{db_id}_{project_id}_{cls}', index=False)
        except:
            with open('errors.csv', 'a') as f:
                f.write(f'{db_id},{project_id},{dev_id}\n')

