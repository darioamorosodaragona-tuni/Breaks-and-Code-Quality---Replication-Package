import os

import pandas as pd

files = os.listdir('../piecewise5')
files = [x for x in files if x.endswith('.csv')]
os.makedirs('../piecewise5_convergent', exist_ok=True
            )
for file in files:
#     with open(f'../piecewise3/{file}', 'r') as f:
#         lines = f.readlines()
#         lines[0] = lines[0].replace(';', '')
#         lines[0] = lines[0].rstrip('\n') + ';' + '\n'
#     with open(f'../piecewise3/{file}', 'w') as f:
#         f.writelines(lines)

    data = pd.read_csv(f'../piecewise5/{file}', sep=",", lineterminator="\n")
    data_notna = data.dropna(inplace=False)
    if data_notna.empty:
        continue
    data.to_csv(f'../piecewise5_convergent/{file}', index=False)