import os
import sqlite3
from datetime import timedelta

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import pytz
import tqdm


def standardize_datetime(dt):
    return dt.tz_convert('UTC')


# Function to adjust the time part of datetime values while preserving timezone information
def adjust_time(dt):
    if pd.isnull(dt):  # Check if the datetime is null
        return dt
    else:
        offset_hours = dt.utcoffset().seconds // 3600  # Calculate offset in hours
        return dt + timedelta(hours=-offset_hours)  # Adjust time by subtracting the offset


comm_per_package = os.listdir('../commits_per_package_concat')

os.makedirs('../commits_per_package_concat_with_delta', exist_ok=True)

for commits in tqdm.tqdm(comm_per_package, desc="Processing packages", position=0, leave=True):
    # Connect to SQLite database
    data = pd.read_csv(f'../commits_per_package_concat/{commits}')
    # utc True to convert to UTC



    try:
        data['dates'] = pd.to_datetime(data['dates'], utc=True)
    except:
        data = data[data['dates'] != 'issue']
        try:
            data['dates'] = pd.to_datetime(data['dates'], utc=True)
        except:
            data['dates'] = pd.to_datetime(data['dates'],format="mixed", utc=True)
    # data['commit_date_standardized'] = data['commit_date'].apply(standardize_datetime)
    data.sort_values(by="dates", inplace=True, ascending=True, ignore_index=True)
    indexes = []
    for index, row in tqdm.tqdm(data.iterrows(), desc="Processing commits", leave=False, position=1):

        if row['hash'] != 'issue':
            indexes.append(index)

        if index == 0:
            data.loc[index, 'delta'] = 0
        else:
            data.loc[index, 'delta'] = (data.loc[index, 'dates'] - data.loc[
                index - 1, 'dates']).days

    for idx in indexes:
        if idx == 0:
            data.loc[idx, 'commits_delta_date'] = 0
        else:
            data.loc[idx, 'commits_delta_date'] = (data.loc[idx, 'dates'] - data.loc[
                indexes[indexes.index(idx)-1], 'dates']).days

    data.to_csv(f'../commits_per_package_concat_with_delta/{commits}', index=False)

