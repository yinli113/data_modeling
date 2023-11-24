#!/usr/bin/env python
# coding: utf-8

# # ETL Processes
# Use this notebook to develop the ETL process for each of tables before completing the `etl.py` file to load the whole datasets.

import os
import glob
import json
import psycopg2
import datetime
import pandas as pd
from sql_queries import *

# Establish a new connection and create a new cursor

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student port=5432")
cur = conn.cursor()

# Use the get_files function provided above to get a list of all song JSON files in data/song_data
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files

filepath = 'data/song_data'
song_files = get_files(filepath)

if song_files:
    song_data_list = []

    for song_file in song_files:
        with open(song_file, 'r') as f:
            for line in f:
                try:
                    song_data = json.loads(line)
                    song_data_list.append(song_data)
                except json.JSONDecodeError:
                    print("Invalid JSON object:", line)

    if song_data_list:
        print(song_data_list[0])
        if len(song_data_list) > 1:
            print(list(song_data_list[1].keys()))
        else:
            print("No records found in song_data_list.")
    else:
        print("No valid JSON data found in the files.")
else:
    print("No JSON files found in the specified directory.")

# convert the string in the list and return it back to dictionary
song_list = [json.loads(item.replace("''", "")) for item in song_data_list]
# get the first record
print(song_list[0])
# get the keys
col = song_list[1].keys()
print(col)

#1: `songs` Table

# create the dataframe
df_song_data = pd.DataFrame(song_list)

# get the values from the dataframe,select just the values from the dataframe
song_data_values = df_song_data.values

# select the first five rows of the dataframe
df_song_data.head()

# Index to select the first (only) record in the dataframe
first_record = df_song_data.loc[0]

# check if the primary key in tables that have been created are unique value
def check_unique(lst):
    if len(lst) == len(set(lst)):
        return True
    else:
        return False

song_id = list(df_song_data['song_id'])
print("song_id is", check_unique(song_id))
print()
artist_id = list(df_song_data['artist_id'])
print("artist_id is", check_unique(artist_id))

# find the duplicated values in artist_id
def find_duplicates(my_list):
    indices = {}
    duplicates = []

    for i, value in enumerate(my_list):
        if value in indices:
            duplicates.append((value, indices[value], i))
        else:
            indices[value] = i

    return duplicates

my_list = df_song_data['artist_id']
duplicates = find_duplicates(my_list)

for value, index1, index2 in duplicates:
    print(f"Value {value} is duplicated at indices {index1} and {index2}")


# #### Insert Record into Song Table
# Implement the `song_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song into the `songs` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songs` table in the sparkify database.

# create the dataframe result which only hold the values of 'song_id'
# 'artist_id', 'year', 'duration', 'title'
selected_columns = ['song_id', 'artist_id', 'year', 'duration', 'title']
result1 = df_song_data[selected_columns]
# create a list with the values of each row in a tuple
song_data = list(zip(result1['song_id'], result1['artist_id'], result1['year'],\
                     result1['duration'], result1['title']))

def insert_data_into_songs(song_data, conn, cur):
    # Insert song data into the songs table
    song_table_insert = "INSERT INTO songs(\
                        song_id, artist_id, year, duration, title)\
                        VALUES(%s, %s, %s, %s, %s)\
                        ON CONFLICT(song_id) DO NOTHING"
    for song in song_data:
        cur.execute(song_table_insert, song)
    conn.commit()
    print("Song data inserted successfully!")

insert_data_into_songs(song_data, conn, cur)

def delete_null_values_in_songs(conn, cur):
    # Delete null values in songs table
    delete_query = "DELETE FROM songs WHERE artist_id IS NULL;"
    cur.execute(delete_query)
    conn.commit()

delete_null_values_in_songs(conn, cur)

def alter_songs_table_columns(conn, cur):
    # Alter the songs table columns to set them to NOT NULL
    alter_queries = [
        "ALTER TABLE songs ALTER duration SET NOT NULL;",
        "ALTER TABLE songs ALTER title SET NOT NULL;",
        "ALTER TABLE songs ALTER song_id SET NOT NULL;",
        "ALTER TABLE songs ALTER artist_id SET NOT NULL;"
    ]
    for query in alter_queries:
        cur.execute(query)
    conn.commit()

alter_songs_table_columns(conn, cur)
# Run `test.ipynb` to see if you've successfully added a record to this table.

# ## #2: `artists` Table
# #### Extract Data for Artists Table
# - Select columns for artist ID, name, location, latitude, and longitude
# - Use `df.values` to select just the values from the dataframe
# - Index to select the first (only) record in the dataframe
# - Convert the array to a list and set it to `artist_data`

# create the dataframe of artists
artist_data_column = ['artist_id', 'artist_name', 'artist_location',\
                      'artist_latitude', 'artist_longitude']
artist_dataframe = df_song_data[artist_data_column]

# replace the empty values in artist_location with NULL
artist_location = [None if i.strip() == ''\
                   else i for i in artist_dataframe['artist_location']]

# get the artists data as tuples in a list
artist_data = list(zip(artist_dataframe['artist_id'],\
                       artist_dataframe['artist_name'], artist_dataframe['artist_location'],\
                       artist_dataframe['artist_latitude'], artist_dataframe['artist_longitude']))

# #### Insert Record into Artist Table
# Implement the `artist_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song's artist into the `artists` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `artists` table in the sparkify database.

def insert_data_into_artists(artist_data, conn, cur):
    # Insert artist data into the artists table
    artist_table_insert = "INSERT INTO artists\
                        (artist_id, artist_name, artist_location,\
                        artist_latitude, artist_longitude)\
                        VALUES (%s, %s, %s, %s, %s)\
                        ON CONFLICT(artist_id)\
                        DO UPDATE SET artist_name = EXCLUDED.artist_name"
    for artist in artist_data:
        cur.execute(artist_table_insert, artist)
    conn.commit()
    print("Artist data inserted successfully!")

insert_data_into_artists(artist_data, conn, cur)

def foreign_key_added(conn, cur):
    # create a foreign key in table songs references to artist table
    foreign_key_alter = "ALTER TABLE songs ADD CONSTRAINT fk_artist\
                        FOREIGN KEY (artist_id)\
                        REFERENCES artists(artist_id)"
    cur.execute(foreign_key_alter)
    conn.commit()


foreign_key_added(conn, cur)

def delete_null_values_in_artists(conn, cur):
    # Delete null values in artists table
    delete_query1 = "DELETE FROM  artists WHERE artist_name IS NULL;"
    delete_query2 = "DELETE FROM  artists WHERE artist_id IS NULL;"
    cur.execute(delete_query1)
    cur.execute(delete_query2)
    conn.commit()

delete_null_values_in_artists(conn, cur)

def set_not_null_values_in_artists(conn, cur):
    # set the column ( artist_id, artist_name)to NOT NULL
    alter_query1 = "ALTER TABLE artists ALTER artist_id SET NOT NULL;"
    alter_query2 = "ALTER TABLE artists ALTER artist_name SET NOT NULL;"
    cur.execute(alter_query1)
    cur.execute(alter_query2)
    conn.commit()

set_not_null_values_in_artists(conn, cur)
# Run `test.ipynb` to see if you've successfully added a record to this table.

# # Process `log_data`
# In this part, you'll perform ETL on the second dataset, `log_data`, to create the `time` and `users` dimensional tables, as well as the `songplays` fact table.
#
# Let's perform ETL on a single log file and load a single record into each table.
# - Use the `get_files` function provided above to get a list of all log JSON files in `data/log_data`
# - Select the first log file in this list
# - Read the log file and view the data

# Use the get_files function provided above to get a list of all song JSON files in data/song_data
def get_file_log(filepath2):
    all_file_log = []
    for root, dirs, files in os.walk(filepath2):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_file_log.append(os.path.abspath(f))

    return all_file_log


filepath2 = 'data/log_data/2018/11'
log_files = get_file_log(filepath2)

log_data_list = []
# Iterate through each log file in the list and read its data
for log_file in log_files:
    with open(log_file, 'r') as f:
        for line in f:
            try:
                dflog = json.loads(line)
                log_data_list.append(line)
            except json.JSONDecodeError:
                print("Invalid JSON object:", line)

print(log_data_list)

# convert the string in the list and return it back to dictionary
log_list = [json.loads(item.replace("''", "")) for item in log_data_list]

# convert the list to a dataframe
df_log = pd.DataFrame(log_list)


# check if the primary key in tables that have been created are unique value
def check_unique(lst):
    if len(lst) == len(set(lst)):
        return True
    else:
        return False


userid = list(df_log['userId'])
print("userid is", check_unique(userid))
print()
ts = list(df_log['ts'])
print("ts is", check_unique(ts))


# compare the two dataframe to see if there is common information to conneting each other, if there is , the table should be altered

# ## #3: `time` Table
# #### Extract Data for Time Table
# - Filter records by `NextSong` action
# - Convert the `ts` timestamp column to datetime
#   - Hint: the current timestamp is in milliseconds
# - Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order
#   - Hint: use pandas' [`dt` attribute](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.html) to access easily datetimelike properties.
# - Specify labels for these columns and set to `column_labels`
# - Create a dataframe, `time_df,` containing the time data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe

# Convert the timestamp to a datetime object
t_time = pd.to_datetime(df_log['ts'])
start_time = [i for i in t_time]

data = {}
column_tables = ['hour', 'day', 'week', 'month', 'year', 'weekday']

# Iterate over the column names and create the lists
for column in column_tables:
    data[column] = [getattr(i, column) for i in t_time]

df_start_time = {'start_time': start_time}
df_artist = df_log['artist']
df_start_time.update(data)
df_start_time.update(df_artist)
# change the dataframe name "df_start_time" to "time_df"
time_df = pd.DataFrame(df_start_time)


# #### Insert Records into Time Table
# Implement the `time_table_insert` query in `sql_queries.py` and run the cell below to insert records for the timestamps in this log file into the `time` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `time` table in the sparkify database.

def insert_time_data(conn, cur, time_df):
    # create insert query
    time_table_insert = "INSERT INTO time (start_time,\
                        hour, day, week, month, year, weekday)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s)\
                        ON CONFLICT (start_time) DO NOTHING"

    # Convert 'start_time' to timestamp with time zone in the DataFrame
    time_df['start_time'] = pd.to_datetime(time_df['start_time'], utc=True)

    # insert data into table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, (
            row['start_time'],
            int(row['hour']),
            int(row['day']),
            int(row['week']),
            int(row['month']),
            int(row['year']),
            str(row['weekday'])
        ))

    # Commit the changes to the database
    conn.commit()
    print("Time data inserted successfully!")


insert_time_data(conn, cur, time_df)
# Run `test.ipynb` to see if you've successfully added records to this table.

# ## #4: `users` Table
# #### Extract Data for Users Table
# - Select columns for user ID, first name, last name, gender and level and set to `user_df`

# change column name userId to userid
df_log.rename(columns={'userId': 'userid'}, inplace=True)

# create dataframe which only hold values in 'userId','firstName','lastName','gender','level','song','userAgent','sessionId'
user_column = ['userid', 'firstName', 'lastName', 'gender', 'level',
               'userAgent', 'sessionId']
user_df = df_log[user_column]


# #### Insert Records into Users Table
# Implement the `user_table_insert` query in `sql_queries.py` and run the cell below to insert records for the users in this log file into the `users` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `users` table in the sparkify database.


def insert_users_data(conn, cur, user_df):
    # create insert query
    user_table_insert = "INSERT INTO users\
        (userid, firstName, lastName, gender,\
        level, userAgent, sessionId)\
        VALUES (%s, %s, %s, %s, %s, %s, %s)\
        ON CONFLICT (userId) DO UPDATE\
        SET level=EXCLUDED.level"

    # insert data into table
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    conn.commit()
    print("users data inserted successfully!")


insert_users_data(conn, cur, user_df)

# Run `test.ipynb` to see if you've successfully added records to this table.

# ## #5: `songplays` Table
# #### Extract Data and Songplays Table
# This one is a little more complicated since information from the songs table, artists table, and original log file are all needed for the `songplays` table. Since the log file does not specify an ID for either the song or the artist, you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
# - Implement the `song_select` query in `sql_queries.py` to find the song ID and artist ID based on the title, artist name, and duration of a song.
# - Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`
#
# #### Insert Records into Songplays Table
# - Implement the `songplay_table_insert` query and run the cell below to insert records for the songplay actions in this log file into the `songplays` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songplays` table in the sparkify database.

# according the matching condition between df_log and df_song_data  to merge two dataframes
# "title" ="song" , "artist_name"="artist", "duration"="length"
# Merge the dataframes based on the specified columns
merged_data = pd.merge(df_song_data, df_log,
                     left_on=["artist_name", "duration", "title"],
                     right_on=["artist", "length", "song"],
                     how="inner")
print(merged_data.keys())
merged_data.info()


# In[ ]:


# change the column name ts to "start_time" and data type to timestamp
merged_data.rename(columns={'ts': 'start_time'}, inplace=True)
# Convert 'date_string' to timestamps
merged_data['start_time'] = pd.to_datetime(merged_data['start_time'])
print(merged_data)


df_log['ts'] = pd.to_datetime(df_log['ts'])


# find a reliable way to link the log_data dataset with the songs and artists tables based on the available data
# By performing this join, you can link the song information from the log_data dataset with the corresponding entries in the songs and artists tables.

def insert_songplay_data(conn, cur, df_song_data, merged_data):
    # insert data into songplays
    songplay_table_insert = """
        INSERT INTO songplays (start_time, userid, level,\
        song_id, artist_id, sessionId, artist_location, userAgent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)\
        ON CONFLICT(songplay_id) DO NOTHING
    """

    song_select = ("""
        SELECT song_id, artists.artist_id
        FROM songs JOIN artists ON songs.artist_id = artists.artist_id
        WHERE artists.artist_name = %s
        AND songs.title = %s
        AND songs.duration = %s
    """)

    for index, row in df_song_data.iterrows():
        cur.execute(song_select, (row.artist_name, row.title, row.duration))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

    for index, row in merged_data.iterrows():
        songplay_data = [row.start_time, row.userid, row.level, song_id,
                            artist_id, row.sessionId, row.artist_location,
                            row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

    conn.commit()
    print("Songplays data inserted successfully!")


insert_songplay_data(conn, cur, df_song_data, merged_data)


# Run `test.ipynb` to see if you've successfully added records to this table.

# # Close Connection to Sparkify Database

cur.close()
conn.close()
