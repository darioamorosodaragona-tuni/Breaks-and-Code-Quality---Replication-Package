import sqlite3
import csv

# Connect to the first SQLite database
conn1 = sqlite3.connect('../dbs/db-split0.sql')
cursor1 = conn1.cursor()

# Connect to the second SQLite database
conn2 = sqlite3.connect('../pull_request_db/db-pl-split0.sqlite')
cursor2 = conn2.cursor()

# Execute the SQL query to select the github_urls from the first database
cursor1.execute('''
    SELECT id, github_url FROM projects
''')

# Fetch all the results from the first database
results1 = cursor1.fetchall()

# Execute the SQL query to select the github_urls from the second database
cursor2.execute('''
    SELECT id, github_url FROM projects
''')

# Fetch all the results from the second database
results2 = cursor2.fetchall()

# Create a dictionary to store the github_urls and corresponding ids from the first database
url_id_map1 = {url: id for id, url in results1}

# Create a dictionary to store the github_urls and corresponding ids from the second database
url_id_map2 = {url: id for id, url in results2}

# Find the matching github_urls and create a list of tuples containing the matching ids
matched_ids = [(url_id_map1[url], url_id_map2[url]) for url in url_id_map1 if url in url_id_map2]

# Write the results to a CSV file
with open('matched_ids.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'pr_id']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for id_1, id_2 in matched_ids:
        writer.writerow({'id': id_1, 'pr_id': id_2})

# Close the connections
conn1.close()
conn2.close()
