import os
import glob
import json
import csv
from cassandra.cluster import Cluster
from create_tables import CqlHelper
from cql_queries import insert_songs_by_session, insert_songs_by_user, insert_users_by_song, select_query1, select_query2, select_query3

def prepare_denormalized_csv(filepath, denorm_file):
    """
    Prepare denormalized csv from given event data csv.

    Parameters:
    filepath (str): even data csv
    denorm_file (str): denormalized filename
    """
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, '*'))

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
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
    """Entry point
    """
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + "/data/event_data/"
    denorm_file = 'event_datafile_new.csv'
    prepare_denormalized_csv(filepath, denorm_file)
    # DB operations
    try:
        cluster = Cluster()
        session = cluster.connect()
        cql_helper = CqlHelper(session, "music_library")
        cql_helper.create_keyspace()
        cql_helper.create_tables()
    except Exception as e:
        print(e)
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()
        return

    # Insert data
    print("Inserting data")
    with open(denorm_file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            try:
                cql_helper.insert_data(insert_songs_by_session, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
                cql_helper.insert_data(insert_songs_by_user, (int(line[10]), int(line[8]),int(line[3]), line[0], line[9], line[1], line[4]))
                cql_helper.insert_data(insert_users_by_song, (line[9], line[0], int(line[10]), line[1], line[4]))
            except Exception as e:
                print(e)
                cql_helper.drop_tables()
                session.shutdown()
                cluster.shutdown()
                return

    # Select data
    try:
        rows = session.execute(select_query1)
        print("Result of Query #1")
        for row in rows:
            print(row)
        print("-----------------------------")
        rows = session.execute(select_query2)
        print("Result of Query #2")
        for row in rows:
            print(row)
        print("-----------------------------")
        rows = session.execute(select_query3)
        print("Result of Query #3")
        for row in rows:
            print(row)
    except Exception as e:
        print(e)
    finally:
        cql_helper.drop_tables()
        session.shutdown()
        cluster.shutdown()
   
if __name__ == "__main__":
    main()
