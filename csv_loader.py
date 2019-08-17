import csv
import shutil
from zipfile import ZipFile
import os
import mysql.connector

# connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="dataloader"
)
db_cursor = db_conn.cursor()

archive_path = '/home/shakir/data/'
archive_name = 'data-archive.zip'
file_name = 'Sacramento_RealEstate_Transactions.csv'

# List available files
# Add: Catch not a directory exception
os.listdir(archive_path)

# Create tmp directory + check if $HOME/temp exists
if not os.path.isdir(os.environ.get('HOME')+'/tmp'):
    os.makedirs(os.environ.get('HOME') + '/tmp')

tmp_dir = os.environ.get('HOME')+'/tmp'

# Extract to $HOME/tmp
with ZipFile(archive_path + archive_name) as archive:
    archive.extractall(path=tmp_dir)

for dir_path, dir_names, file_names in os.walk(tmp_dir):
    for file_ in file_names:
        fn = os.path.join(dir_path, file_)
        print(fn)  # print extracted file(s)

# Choose a file:
# csv_select = input("Type in a csv file to insert to DB...\n")

# Join path + file name
final_csv_file = os.path.join(tmp_dir, file_name)

# Create table

sql = "DROP TABLE IF EXISTS TRANSACTIONS"
db_cursor.execute(sql)
print("Table: TRANSACTIONS dropped successfully.\n")

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
    print("Table EMP created successfully.\n")

except mysql.connector.Error as err:
    print("SQL Error in CREATE TABLE segment:\n", "{}".format(err), "\n")


# Assign to csv reader
with open(final_csv_file, newline='') as csv_file:
    reader = csv.DictReader(csv_file)
    # Insert to DB
    for row in reader:
        try:
            sql = "INSERT INTO TRANSACTIONS (STREET, CITY, ZIP, STATE, BEDS, BATHS, SQ_FT, TYPE, SALE_DATE, PRICE, LATITUDE, LONGITUDE) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (row['street'], row['city'], row['zip'], row['state'],row['beds'], row['baths'], row['sq__ft'], row['type'],row['sale_date'], row['price'], row['latitude'], row['longitude'])

            db_cursor.execute(sql, val)
            db_conn.commit()
            print(db_cursor.rowcount, " record inserted.\r")

        except mysql.connector.Error as err:
            print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")

#Close DB Connection
db_conn.close
db_cursor.close()

# Delete $HOME/tmp
shutil.rmtree(tmp_dir)
print("Directory\t", tmp_dir, "\tdeleted successfully")

