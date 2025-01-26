import pandas as pd
import numpy as np
import pymysql
import sys

# Get the file path from command line arguments
file_path = sys.argv[1]

# Read the CSV file using pandas
user_info_ = pd.read_csv(file_path, sep=',')

user_info_['Season'] = user_info_['Title'].str.extract(r'(Season \d+)|(Part \d+)|(Book \d+)|(Volume \d+)|(Limited Series \d+)|(Chapter \d+)').fillna('').sum(axis=1)
user_info_['Episode'] = user_info_['Title'].str.extract(r'(Episode \d+)')
user_info_['Title'] = user_info_['Title'].str.strip()
user_info_['id'] = range(1, len(user_info_) + 1)
tv_keywords = ['Limited Series', 'Season', 'Book', 'Volume', 'Chapter', 'Episode', 'Part']

# Create a boolean mask to identify rows containing TV titles
tv_mask = user_info_['Title'].str.contains('|'.join(tv_keywords), case=False)

user_info_['titleType'] = np.where(tv_mask, 'tv', 'movie')
pattern = '|'.join(tv_keywords)

# Remove the substring after the keywords
user_info_['Title'] = user_info_['Title'].str.split(pattern).str[0].str.rstrip(': ')

user_info_['Title'] = user_info_['Title'].str.replace(r'(: Season \d+)', '').str.replace(r'(: Episode \d+)', '').str.replace(r'(: Part \d+)', '').str.replace(r'(: Limited Series \d+)', '').str.replace(r'(: Book \d+)', '')

# Connection parameters
connection_params = {
    'host': '35.238.150.143',
    'user': 'root',
    'password': 'team117',
    'database': 'netflix_wrapped'
}

# Connect to the database
connection = pymysql.connect(**connection_params)

try:
    # Create a temporary table in the database
    with connection.cursor() as cursor:
        sql_query = "DELETE FROM user_info"
        # Execute the query
        cursor.execute(sql_query)

    # Insert data from the DataFrame into the temporary table
    with connection.cursor() as cursor:
        for index, row in user_info_.iterrows():
            if(pd.isnull(row['Title'])):
                continue

            if not pd.isnull(row['Season']) and not pd.isnull(row['Episode']):
                insert_query = "INSERT INTO user_info (Title, Date, Season, Episode, titleType, id) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (row['Title'], row['Date'], row['Season'], row['Episode'], row['titleType'], row['id']))
            elif pd.isnull(row['Season']) and not pd.isnull(row['Episode']):
                insert_query = "INSERT INTO user_info (Title, Date, Episode, titleType, id) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (row['Title'], row['Date'], row['Episode'], row['titleType'], row['id']))
            elif pd.isnull(row['Episode']) and not pd.isnull(row['Season']):
                insert_query = "INSERT INTO user_info (Title, Date, Season, titleType, id) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (row['Title'], row['Date'], row['Season'], row['titleType'], row['id']))
            # else:
            #     insert_query = "INSERT INTO user_info (Title, Date, titleType, id) VALUES (%s, %s, %s, %s)"
            #     # print()
            #     cursor.execute(insert_query, (row['Title'], row['Date'], row['titleType'], row['id']))

    # Commit the changes
    connection.commit()

    print("Data inserted successfully into the temporary table.")

finally:
    # Close the connection
    connection.close()
