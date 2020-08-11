"""
Python script for zip file extract & load into MySQL table
"""
import csv
import shutil
import sys
from zipfile import ZipFile
import os
import mysql.connector

# connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",  # not a pswd
    database="dataloader"
)
db_cursor = db_conn.cursor()

archive_path = '/home/shakir/data/'
archive_name = 'data-archive.zip'
file_name = 'Sacramento_RealEstate_Transactions.csv'


def unzip_file():
    # First check if $HOME/temp exists, if not then create one
    if not os.path.isdir(os.environ.get('HOME')+'/tmp'):
        os.makedirs(os.environ.get('HOME') + '/tmp')

    tmp_dir = os.environ.get('HOME')+'/tmp'

    # Extract to $HOME/tmp
    with ZipFile(os.path.join(archive_path, archive_name)) as archive:
        archive.extractall(path=tmp_dir)
    # List extracted content
    print("Extracted files:")
    for dir_path, dir_names, file_names in os.walk(tmp_dir):
        for file_ in file_names:
            fn = os.path.join(dir_path, file_)
            print(fn)
    # Join tmp folder path + file name
    # For CSV file reading in DB function
    final_csv_file = os.path.join(tmp_dir, file_name)
    return final_csv_file, tmp_dir

# From the previous function,
# pass in: unzipped csv file and temporary directory path


def db_processing(csv_file_in, temporary_directory):
    # Create table
    sql = "DROP TABLE IF EXISTS TRANSACTIONS"
    db_cursor.execute(sql)
    print("\nTable: TRANSACTIONS dropped successfully.\n")
    try:
        sql = "CREATE TABLE TRANSACTIONS " \
              "(ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT, " \
              "STREET VARCHAR(245), " \
              "CITY  VARCHAR(245), " \
              "ZIP  VARCHAR(245), " \
              "STATE CHAR(2), " \
              "BEDS FLOAT, " \
              "BATHS FLOAT, " \
              "SQ_FT FLOAT, " \
              "TYPE VARCHAR(245), " \
              "SALE_DATE VARCHAR(245), " \
              "PRICE DOUBLE, " \
              "LATITUDE DOUBLE, " \
              "LONGITUDE DOUBLE);"
        db_cursor.execute(sql)
        db_conn.commit()
        print("Table TRANSACTIONS created successfully.\n")

    except mysql.connector.Error as err:
        print("SQL Error in CREATE TABLE segment:\n", "{}".format(err), "\n")

    # Read unzipped csv file
    with open(csv_file_in, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        print('Processing . . .')

        # Insert into DB
        try:
            for row in reader:
                sql = "INSERT INTO TRANSACTIONS (STREET, CITY, ZIP, STATE, BEDS, BATHS, SQ_FT, TYPE, SALE_DATE, PRICE, LATITUDE, LONGITUDE) " \
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (row['street'], row['city'], row['zip'], row['state'], row['beds'], row['baths'],
                       row['sq__ft'], row['type'], row['sale_date'], row['price'], row['latitude'], row['longitude'])
                db_cursor.execute(sql, val)

                sys.stdout.write('Processing \r    ')
                sys.stdout.flush()

        except mysql.connector.Error as err:
            print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")
    # Close DB Connection
    db_conn.commit()
    db_conn.close
    db_cursor.close()

    print("\nRecords inserted successfully.")

    # Delete $HOME/tmp
    shutil.rmtree(temporary_directory)
    print("Temporary directory:\t", temporary_directory, "\t- deleted successfully")


def main():
    final_csv_file, tmp_dir = unzip_file()
    db_processing(final_csv_file, tmp_dir)


if __name__ == '__main__':
    main()
