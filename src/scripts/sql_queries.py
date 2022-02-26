# CREATE TABLES
songplays_create_table = """
                CREATE TABLE IF NOT EXISTS songplays(
                    songplay_id SERIAL PRIMARY KEY,
                    start_time timestamp NOT NULL,
                    user_id varchar NOT NULL,
                    level varchar,
                    song_id varchar NOT NULL,
                    artist_id varchar NOT NULL,
                    session_id int,
                    location varchar,
                    user_agent varchar
                )
                """

users_create_table = """
               CREATE TABLE IF NOT EXISTS users(
                    user_id varchar PRIMARY KEY,
                    first_name varchar NOT NULL,
                    last_name varchar NOT NULL,
                    gender varchar(1),
                    level varchar NOT NULL 
               ) 
               """


songs_create_table = """
               CREATE TABLE IF NOT EXISTS songs(
                    song_id varchar PRIMARY KEY,
                    title varchar NOT NULL ,
                    artist_id varchar NOT NULL ,
                    year int,
                    duration int
               ) 
               """

artists_create_table = """
               CREATE TABLE IF NOT EXISTS artists(
                    artist_id varchar PRIMARY KEY ,
                    name varchar NOT NULL ,
                    location varchar,
                    latitude float,
                    longitude float
               ) 
               """

time_create_table = """
               CREATE TABLE IF NOT EXISTS time(
                    start_time timestamp PRIMARY KEY,
                    hour int NOT NULL,
                    day int NOT NULL ,
                    week int NOT NULL ,
                    month int NOT NULL ,
                    year int NOT NULL ,
                    weekday varchar NOT NULL 
               ) 
               """


# DROP TABLES
songplays_drop_table = "DROP TABLE IF EXISTS songplays"
users_drop_table = "DROP TABLE IF EXISTS users"
songs_drop_table = "DROP TABLE IF EXISTS songs"
artists_drop_table = "DROP TABLE IF EXISTS artists"
time_drop_table = "DROP TABLE IF EXISTS time"


create_tables_list = [songplays_create_table, users_create_table, songs_create_table, artists_create_table,
                      time_create_table]
drop_tables_list = [songplays_drop_table, users_drop_table, songs_drop_table, artists_drop_table, time_drop_table]

# INSERT RECORD

songplays_insert_table = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES(to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
                         """

users_insert_table = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE 
    SET level = EXCLUDED.level
"""

song_insert_table = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
""")

artist_insert_table = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
""")

time_insert_table = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES(to_timestamp(%s), %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id 
    FROM songs JOIN artists 
        ON songs.artist_id=artists.artist_id
    WHERE songs.title=%s AND artists.name=%s;
""")