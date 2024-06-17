import os
import sqlite3
import pandas as pd

clusters = os.listdir("../13.b Developers Interactions/cluster_results")
clusters = [x for x in clusters if x.endswith('.csv') and x != 'result_all_together.csv']
os.makedirs('clusters_with_ids', exist_ok=True)

for file in clusters:
    df = pd.read_csv(f'../13.b Developers Interactions/cluster_results/{file}')

    db_id = file.split('db-split')[1].split('clusters.csv')[0]
    db_to_connect = "/Users/ncdaam/PycharmProjects/ForgettingCode/Data/dbs/db-split" + db_id + ".sql"
    conn = sqlite3.connect(db_to_connect)

    # Constructing placeholders for each row in the dataframe
    placeholders = ', '.join(['(?, ?)' for _ in range(len(df))])

    # Creating a temporary table to hold the dataframe values
    temp_table_name = 'temp_df'
    df.to_sql(temp_table_name, conn, if_exists='replace', index=False)

    # Joining the temporary table with gh_users to get the git_user_id
    query = f"""
          SELECT df.*, gu.id AS git_user_id
          FROM {temp_table_name} df
          LEFT JOIN git_users gu ON df.name = gu.author_name AND df.project_id = gu.project_id
      """
    try:
        result = pd.read_sql_query(query, conn)
        result.to_csv(f'clusters_with_ids/{file}', index=False)
    except Exception as e:
        print(f"Error in {file}: {e}")

    # Dropping the temporary table
    conn.execute(f"DROP TABLE {temp_table_name}")
    conn.commit()

    conn.close()
