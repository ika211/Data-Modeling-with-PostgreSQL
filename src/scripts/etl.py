import os
import pandas as pd
import psycopg2
import glob
from src.scripts.sql_queries import *
from src.scripts.create_tables import main as create_tables_proc


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def insert_table_from_df(df, cur, conn, sql_insert_query):
    for i, row in df.iterrows():
        cur.execute(sql_insert_query, row)
        conn.commit()


def main():
    create_tables_proc()

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=postgres password=password")
    cur = conn.cursor()

    path_songs = '../../data/song_data'
    path_logs = '../../data/log_data'

    files_song = get_files(path_songs)
    files_log = get_files(path_logs)

    n_songs = len(files_song)
    n_logs = len(files_log)

    songs_df = pd.read_json(files_song[0], lines=True)
    for i in range(1, n_songs):
        temp = pd.read_json(files_song[i], lines=True)
        songs_df = pd.concat([songs_df, temp])

    logs_df = pd.read_json(files_log[0], lines=True)
    for i in range(1, n_logs):
        temp = pd.read_json(files_log[i], lines=True)
        logs_df = pd.concat([logs_df, temp])

    songs_df['duration'] = round(songs_df['duration'])
    insert_data_songs_df = songs_df[['song_id', 'title', 'artist_id', 'year', 'duration']]

    insert_table_from_df(insert_data_songs_df, cur, conn, song_insert_table)
    print("--------------------------------")
    print("  INSERTED ROWS TO SONGS TABLE  ")
    print("--------------------------------")

    insert_data_artists_df = songs_df[['artist_id', 'artist_name', 'artist_location',
                                       'artist_latitude', 'artist_longitude']]
    insert_table_from_df(insert_data_artists_df, cur, conn, artist_insert_table)
    print("--------------------------------")
    print(" INSERTED ROWS TO ARTISTS TABLE ")
    print("--------------------------------")

    # logs data
    logs_df = logs_df[logs_df['page'] == 'NextSong']

    # Create time dataframe to insert into Table
    ts = pd.to_datetime(logs_df['ts'], unit="ms")
    insert_data_time = [(round(dt.timestamp()), dt.hour, dt.day, dt.week, dt.month,
                         dt.year, dt.day_name()) for dt in ts]
    time_columns = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    insert_data_time_df = pd.DataFrame(insert_data_time, columns=time_columns)

    # Insert DF rows to Time table
    insert_table_from_df(insert_data_time_df, cur, conn, time_insert_table)
    print("--------------------------------")
    print("  INSERTED ROWS TO TIME TABLE   ")
    print("--------------------------------")

    # Create users dataframe to insert into Table
    users_df = logs_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    users_df = users_df[users_df['userId'].astype(bool)]
    insert_data_users_df = users_df.drop_duplicates(subset='userId')

    # Insert DF rows to Users table
    insert_table_from_df(insert_data_users_df, cur, conn, users_insert_table)
    print("--------------------------------")
    print("  INSERTED ROWS TO USERS TABLE  ")
    print("--------------------------------")

    # Inserting songplay records
    for i, row in logs_df.iterrows():

        song_name = row.song
        artist_name = row.artist
        song_duration = round(row.length)

        cur.execute(song_select, (song_name, artist_name))
        result = cur.fetchone()
        (song_id, artist_id) = (result if result else (None, None))

        if song_id is None and artist_id is None:
            continue

        songplay_data = (round(row.ts / 1000.0), row.userId, row.level,
                         song_id, artist_id, row.sessionId, row.location,
                         row.userAgent)

        cur.execute(songplays_insert_table, songplay_data)
        conn.commit()
    print("--------------------------------")
    print("INSERTED ROWS TO SONGPLAYS TABLE")
    print("--------------------------------")

    conn.close()


if __name__ == '__main__':
    main()
