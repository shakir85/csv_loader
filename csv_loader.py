import csv
import shutil
from zipfile import ZipFile
import os
import mysql.connector

# connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="dataloader"
)
my_cursor = mydb.cursor()

archive_path = '/home/shakir/data/'
archive_name = 'dummy.zip'
file_name = 'dummy.csv'

# List available files
# Add: Catch not a directory exception
os.listdir(archive_path)

# Create tmp directory
# Add: if .. else to check if dir exists
os.makedirs(os.environ.get('HOME')+'/tmp')

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

# Assign to csv reader
with open(final_csv_file, newline='') as csv_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
        try:
            sql = "INSERT INTO emp (ID, NAME, SALARY, DEPT) VALUES (%s, %s, %s, %s)"
            val = (row['id'], row['name'], row['salary'], row['department'])
            my_cursor.execute(sql, val)
            mydb.commit()
            print(my_cursor.rowcount, " record inserted.")
        except mysql.connector.Error as err:
            print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")

# Delete $HOME/tmp
shutil.rmtree(tmp_dir)

