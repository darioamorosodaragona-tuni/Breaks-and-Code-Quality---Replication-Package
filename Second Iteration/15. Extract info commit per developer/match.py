import os

import pandas as pd


def parse_file(filepath):
    data = {}
    with open(filepath, 'r') as f:
        lines = f.readlines()
        header = lines[0].strip().split(',')
        for line in lines[1:]:
            parts = line.strip().split(',')
            package_name = parts[0]
            db = int(parts[1])
            project_id = int(parts[2])
            package_id = int(parts[3])
            key = (package_name, db, project_id)
            if key in data:
                data[key].append(package_id)
            else:
                data[key] = [package_id]
    return data


def find_matching_files(folder1, folder2):
    file1_data = parse_file(folder1)
    file2_data = parse_file(folder2)

    matches = 'package_name,db,project_id,package_id,package_id_pr\n'
    for key in file1_data:
        if key in file2_data:
            matches += f"{key[0]},{key[1]},{key[2]},{file1_data[key][0]},{file2_data[key][0]}\n"
    return matches


if __name__ == "__main__":

    files1 = '../package_ids.csv'
    files2 = '../package_ids_pr.csv'
    data= pd.read_csv(files1)
    data.drop_duplicates(inplace=True)
    data.to_csv(files1, index=False)
    data = pd.read_csv(files2)
    data.drop_duplicates(inplace=True)
    data.to_csv(files2, index=False)

    matching_files = find_matching_files(files1, files2)
    with open('matching_files.csv', 'w') as f:
        f.write(matching_files)
    # print("Matching files in folder1 and folder2:")
    # for match in matching_files:
    #     print(match)

# 1. creare packeg_ids.csv senza pull request e issues
# 2. creare package_ids_pr.csv per pull request
# 3. eseguire match.py
# 4. eseguire merge.py
# 5. associare pull request


