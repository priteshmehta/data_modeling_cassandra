import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
from create_tables import CqlHelper

def prepare_denormalized_csv(filepath):
    """
    """
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, '*'))

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

    # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

    # extracting each data row one by one and append it
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    print("Total Records: {}", len(full_data_rows_list))
    
    # Creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                    'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print("Total CSV rows: {}".format(sum(1 for line in f)))
        
def main():
    """
    """
    # Get your current folder and subfolder event data
    #filepath = os.getcwd() + dir_path
    #prepare_denormalized_csv(filepath)
    setup_db_cluster()

def setup_db_cluster():
    """
    """
    cluster = Cluster()
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    cql_helper = CqlHelper(session, "music_library")
    cql_helper.create_keyspace()
    cql_helper.create_tables()
    print("TABLES:", cluster.metadata.keyspaces["music_library"].tables)
    cql_helper.drop_tables()
    print("TABLES:", cluster.metadata.keyspaces["music_library"].tables)

    '''
    # Create & Set Keyspace 
    # NetworkTopologyStrategy
    create_keyspace = "CREATE KEYSPACE IF NOT EXISTS music_library WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '3'}"
    session.execute(create_keyspace)
    session.set_keyspace('music_library')

    create_table1 = "CREATE TABLE IF NOT EXISTS played_songs_by_session (session_id int, item_in_session int, artist_name text, song_title text, song_length decimal, PRIMARY KEY(session_id, item_in_session))"
    create_table2 = "CREATE TABLE IF NOT EXISTS played_songs_by_user (user_id int, session_id int, artist_name text, item_in_session int, song_title text, user_fname text, user_lname text, PRIMARY KEY((user_id, session_id), item_in_session))"
    create_table3 = "CREATE TABLE IF NOT EXISTS user_detail_by_song (song_title text, artist_name text, user_fname text, user_lname text, PRIMARY KEY(song_title, artist_name))"
    try:
        session.execute(create_table1)
        session.execute(create_table2)
        session.execute(create_table3)
        print("TABLES:", cluster.metadata.keyspaces["music_library"].tables)
        #print(rows)
        print("tables created successfully")
    except Exception as e:
        print(e)
    finally:
        session.shutdown()
        cluster.shutdown()
    '''
    '''
    Create queries to ask the following three questions of the data
    1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
    2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    
    artist
    firstName of user
    gender of user
    item number in session
    last name of user
    length of the song
    level (paid or free song)
    location of the user
    sessionId
    song title
    userId

    Sharpe & The Magnetic Zeros","Chloe","F","44","Cuevas","306.31138","paid","San Francisco-Oakland-Hayward, CA","648","Home","49"
    '''





if __name__ == "__main__":
    main()

