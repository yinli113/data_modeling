# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES
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

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays
                        (start_time,
                        userid,
                        level,
                        song_id,
                        artist_id,
                        sessionId,
                        artist_location,
                        userAgent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(songplay_id) DO NOTHING
""")

user_table_insert = ("""INSERT INTO users(userid, firstName,\
                    lastName, gender, level, userAgent, sessionId) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s)\
                    ON CONFLICT (userId) DO UPDATE SET \
                    level=EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs(song_id, artist_id, year, duration, title)
                         VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT(song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, artist_name, artist_location,
                       artist_latitude, artist_longitude)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT(artist_id)
                       DO UPDATE SET artist_name = EXCLUDED.artist_name
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT song_id, artists.artist_id
        FROM songs JOIN artists ON songs.artist_id = artists.artist_id
        WHERE artists.artist_name = %s
        AND songs.title = %s
        AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]