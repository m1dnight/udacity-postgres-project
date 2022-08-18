import glob
import os
from datetime import datetime
import numpy
import pandas
import pandas as pd
import psycopg2
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes a song file from data/song_data by parsing it from JSON. 
    Inserts records into artist table and song table. 

    cur     : database cursor
    filepath: path to the json file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.replace({numpy.nan: None})  # replace nulls in long/lats that are missing.
    df = df.replace({numpy.empty: None})  # replace nulls in long/lats that are missing.
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


# noinspection PyPackageRequirements
def process_log_file(cur, filepath):
    """Processes a log file from the data/log_data directory by parsing it from JSON. 
    Inserts records into the time, user, and songplay table.

    cur     : database cursor
    filepath: path to the json file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_data = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    # t is a Series of timestamps.
    t = pandas.to_datetime(log_data['ts'], unit='ms')

    # insert time data records (https://knowledge.udacity.com/questions/403616)
    # time_data is a list of lists, where the first list is the timestamp value, second is the hour, etc.
    time_data = [t,
                 t.dt.hour,
                 t.dt.day,
                 t.dt.isocalendar().week,
                 t.dt.month,
                 t.dt.year,
                 t.dt.dayofweek]

    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'dayofweek']
    time_df = pandas.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_data[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        if row[0] != '':
            cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in log_data.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
