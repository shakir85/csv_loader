import csv
from zipfile import ZipFile
import os

archive_path = '/home/shakir/data/'
archive_name = '2dummies.zip'
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
csv_select = input("Type in a csv file to insert to DB...\n")

# Join path + file name
final_csv_file = os.path.join(tmp_dir, csv_select)

# Assign to csv reader
with open(final_csv_file, newline='') as csv_file:
    reader = csv.DictReader(csv_file)

# continue ...
