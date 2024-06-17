# Importa le librerie necessarie
import os
import warnings
from tqdm import tqdm
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

os.makedirs('correlations', exist_ok=True)


for file in tqdm(os.listdir("files_per_clusters"), position=0, leave=True, desc="files"):
    data = pd.read_csv(f'files_per_clusters/{file}')

    # if "super active" in file:
    #     print()

    delta_days = data['action_delta_date']
    cols = data.columns.tolist()
    cols = [x for x in cols if x.startswith('delta_')]


    cls = file.split('_')[-1]

    for col in tqdm(cols, position=1, leave=False, desc="cols"):

        # if data[col].isnull().values.all():
        #     continue

            # Create a boolean mask for rows without NaN in the current column
        mask = ~data[col].isna()

        # Filter the column data and delta_days using the mask
        not_na_data = data[col][mask]
        X = delta_days[mask]

        y = np.array(not_na_data.tolist())
        X = np.array(X.tolist())


        correlation_coefficient = np.corrcoef(X, y)[0,1]
        sample_size = len(X)


        with open(f"correlations/{file}.csv", "a") as f:
            f.write(f"{cls},{col},{correlation_coefficient},{sample_size}\n")

import zipfile
import os

def zip_folder(folder_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)

# Example usage
folder_to_zip = 'correlations'
zip_file_path = 'correlations.zip'

zip_folder(folder_to_zip, zip_file_path)

