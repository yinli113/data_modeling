#!/usr/bin/env python
# coding: utf-8

# CREATE DATABASE AND TABLE
# 

# In[1]:


import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# Next use that connect to get a cursor that we will use to execute queries.

# In[2]:



def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


# In[3]:


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS songs"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS times"
    
    drop_table_queries = [
        songplay_table_drop,
        user_table_drop,
        song_table_drop,
        artist_table_drop,
        time_table_drop
    ]
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# In[4]:


import psycopg2

# Define the create_table function
def create_table(cur, conn):
    
# CREATE statements for each table
    song_table_create = """
                CREATE TABLE IF NOT EXISTS songs(
                song_id VARCHAR PRIMARY KEY,
                artist_id VARCHAR,
                year INT,
                duration NUMERIC,
                title VARCHAR
    );
    """

    artist_table_create = """
                CREATE TABLE IF NOT EXISTS artists (
                artist_id text PRIMARY KEY NOT NULL,
                artist_name text,
                artist_location text ,
                artist_latitude float,
                artist_longitude float
    );
    """

    time_table_create = """
                CREATE TABLE IF NOT EXISTS time (
                start_time TIMESTAMP PRIMARY KEY NOT NULL,
                hour INT,
                day INT,
                week INT,
                month INT,
                year INT,
                weekday VARCHAR
    );
    """

    user_table_create = """
                CREATE TABLE IF NOT EXISTS users (
                userid VARCHAR PRIMARY KEY NOT NULL,
                firstName VARCHAR,
                lastName VARCHAR,
                gender VARCHAR,
                level VARCHAR,
                userAgent VARCHAR,
                sessionId INT
    );
    """

    songplay_table_create = """
                CREATE TABLE IF NOT EXISTS songplays (
                songplay_id SERIAL PRIMARY KEY, 
                start_time TIMESTAMP REFERENCES time(start_time) , 
                userid VARCHAR  REFERENCES users(userid), 
                level VARCHAR , 
                song_id VARCHAR REFERENCES songs(song_id), 
                artist_id VARCHAR REFERENCES artists(artist_id), 
                sessionId INT, 
                artist_location VARCHAR, 
                userAgent VARCHAR
    );
    """

# List of CREATE statements for each table
    create_table_queries = [song_table_create, artist_table_create, time_table_create, user_table_create, songplay_table_create]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# In[5]:


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_table(cur,conn )

    conn.close()


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




