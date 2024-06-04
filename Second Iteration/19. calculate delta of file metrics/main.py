import os
import sqlite3
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from tqdm import tqdm

os.makedirs('../files_per_developer_with_metric_and_sonar_with_delta', exist_ok=True)

files = os.listdir('../files_per_developer_with_metric_and_sonar')

for file in tqdm(files, position=1, leave=False, desc="Processing packages"):

    # Read the CSV file into a Pandas DataFrame
    file_df = pd.read_csv(f'../files_per_developer_with_metric_and_sonar/{file}')
    file_df.drop_duplicates(inplace=True)

    # Iterate over rows in the DataFrame and construct SQL queries dynamically

    file_df.fillna(0, inplace=True)
    cols = file_df.columns.tolist()

    to_remove = ['git_file_id', 'worker_id', "git_commit_id", "filename", "old_path", "new_path", "new_path_modified",
                 "package",
                 "commits_delta_date", 'pr', 'sonar_ids', 'sonar_measures_id','revision_id','index']
    cols = [x for x in cols if x not in to_remove]

    for index, row in file_df.iterrows():
        for col in cols:
            if index == 0:
                file_df.loc[index, "delta_" + col] = 0
            else:
                try:
                    file_df.loc[index, "delta_" + col] = (file_df.loc[index, col] - file_df.loc[index - 1, col])
                except:
                    pass

    file_df.to_csv(f'../files_per_developer_with_metric_and_sonar_with_delta/{file}', index=False)
