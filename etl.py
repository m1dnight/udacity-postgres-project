import glob
import os

import numpy
import pandas
import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    """Processes a song file from data/song_data by parsing it from JSON. 
    Inserts records into artist table and song table. 

    :param cur: database cursor.
    :param filepath: path to the json file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.replace({numpy.nan: None})  # replace nulls in long/lats that are missing.
    df = df.replace({numpy.empty: None})  # replace nulls in long/lats that are missing.
    process_songs(cur, df)
    process_artists(cur, df)


def process_artists(cur, df):
    """
    Processes a dataframe with artist data.

    :param cur: database cursor.
    :param df: Dataframe containing artist data.
    """
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_songs(cur, df):
    """
    Processes a song dataframe.
    :param cur: database cursor.
    :param df: Dataframe containing song data.
    """
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def process_users(cur, df):
    """
    Processes a user dataframe.
    :param cur: a database cursor
    :param df: Dataframe containing user data.
    """
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # insert user records
    for i, row in user_df.iterrows():
        if row[0] != '':
            cur.execute(user_table_insert, row)


def process_songplays(cur, df):
    """
    Processes a songplay dataframe.
    :param cur: database cursor.
    :param df: Dataframe containing songplay events.
    """
    # insert songplay records
    for index, row in df.iterrows():
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


# noinspection PyPackageRequirements
def process_log_file(cur, filepath):
    """Processes a log file from the data/log_data directory by parsing it from JSON. 
    Inserts records into the time, user, and songplay table.

    cur     : database cursor
    filepath: path to the json file.
    """
    # open log file
    df = log_to_json(filepath)

    # filter by NextSong action
    log_data = df.loc[df['page'] == "NextSong"]

    process_times(cur, log_data)
    process_users(cur, log_data)
    process_songplays(cur, log_data)


def process_times(cur, df):
    """
    Processes time logs.
    :param cur: database cursor
    :param df: Dataframe that contains logs wth timestamps.
    """
    # convert timestamp column to datetime
    # t is a Series of timestamps.
    t = pandas.to_datetime(df['ts'], unit='ms')
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


def log_to_json(filepath):
    """
    Parses a log file into a dataframe.
    :param filepath: Path to json file.
    :return: A dataframe.
    """
    df = pd.read_json(filepath, lines=True)
    return df


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
