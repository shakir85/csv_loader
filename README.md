# CSV Files Loader

## About

Python program to unzip locally hosted archive file and then load a specific CSV file to MySQL table. This program is intended to demonistrate the concept of data extracting and loading tasks. The program is strcutured to handle a historical real estate prices data set of Sacramento county, California. However, the script can be altered to manage similar data extraction jobs. The program assumes that the user already has the read & write permissions to `HOME` directory and to MySQL DB Server.

## Program Approach

_How to tackle the problem and why?_

The approach is to dump the unzipped content to a temporary folder on disk, do the database insert task, and then get rid of the temp folder to eliminate redundant data on disk.

Writing unzipped content to a disk rather than holding them in memory via using `StringIO` and `BytesIO` streams seems less efficient, but that's not true from memory management point of view.

Holding intermediate data on memory will fail for the following reasons:

- Server instances will run out of memory with huge data sets.
- Risk of cost-increase when working on cloud instance (i.e. AWS EC2) while memory auto-scaling is enabled.
- Disks are cheaper than memory in terms of cost & failures, as long as proper configurations of distributed storage and distributed filesystem on place.
- Unzipping files on disk allows us to chain multiple files unzipping-tasks in a single program run.

However, there are some trade-offs of using disks, such as:

- Network congestion.
- Slower disk IO (especially with non-SSD disks).
- Performance dependability on storage and filesystem architectures.
- If reading/writing to/from cloud storage, additional factors should be considered such as cost, storage configurations, retrieving data from archiving systems ...etc

### Program Structure

_Per the code sequence:_

- **First function (Files Processing):**

  - Walk through the file system to select specific zip archive.
  - Creates a temporary directory in disk to hold unzipped content.
  - Selects the desired CSV file to be loaded into a MySQL table.

- **Second function (Database Tasks):**
  - Connect to a local MySQL DB server.
  - Create specific table based on the CSV file schema.
  - Iterate through the CSV file and insert the data into MySQL table.
  - Delete temporary directory.

Note: Because input paths, archive and file names are hardcoded, I intintionally avoided adding exception handeling blocks for these parts.

## Script Explanation

Each step is explained briefly:

### Function 1

- To create a temporary folder we need to check if there's an existing one in the similar path:

```python
# Check first if $HOME/temp exists
if not os.path.isdir(os.environ.get('HOME')+'/tmp'):
    # Then create ...
    os.makedirs(os.environ.get('HOME') + '/tmp')
```

- Assign the newly create temp directory to a variable:

```python
tmp_dir = os.environ.get('HOME')+'/tmp'
```

- Now we can extract the zip archive:

```python
# Extract to $HOME/tmp
with ZipFile(archive_path + archive_name) as archive:
    archive.extractall(path=tmp_dir)
```

- Print to user the extracted content (_optional step_):

```python
# List extracted content
print("Extracted files:")
for dir_path, dir_names, file_names in os.walk(tmp_dir):
    for file_ in file_names:
        fn = os.path.join(dir_path, file_)
        print(fn)
```

- Finally, join the temp-directory's path with the CSV file's name to make an absolute path for that CSV file so we can pass it to CSV reader object later on. The function are returning to objects:

1. The absolute path of the unzipped CSV file.
2. The absolute path of the temporary directory (to delete it when we finish DB insert task)

```python
final_csv_file = os.path.join(tmp_dir, file_name)
    # returns of files processing function
    return final_csv_file, tmp_dir
```

### Function 2

- Drop any similar tables & create fresh one (I'm not a big fan of dropping tables, but this is for testing only!).

```python
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
```

- Catch SQL table-create level exceptions:

```python
    except mysql.connector.Error as err:
        print("SQL Error in CREATE TABLE segment:\n", "{}".format(err), "\n")
```

- Read the unzipped CSV file (passed from the previous function)

```python
    with open(csv_file_in, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        print('Processing . . .')
```

- Iterate through the CSV file and insert the data into our table:

```python
for row in reader:
    try:
        sql = "INSERT INTO TRANSACTIONS (STREET, CITY, ZIP, STATE, BEDS, BATHS, SQ_FT, TYPE, SALE_DATE, PRICE, LATITUDE, LONGITUDE) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row['street'], row['city'], row['zip'], row['state'],row['beds'], row['baths'], row['sq__ft'], row['type'],row['sale_date'], row['price'], row['latitude'], row['longitude'])
        db_cursor.execute(sql, val)
```

- Catch SQL insert-level exceptions:

```python
except mysql.connector.Error as err:
    print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")
```

- Commit and safely close DB connection:

```python
db_conn.commit()
db_conn.close
db_cursor.close()

# Confirm to user
print("\nRecords inserted successfully.")
```

- Now we've successfully inserted the data, we don't need the temporary directory any more:

```python
# Delete $HOME/tmp
shutil.rmtree(temporary_directory)
print("Temporary directory:\t", temporary_directory, "\t- deleted successfully")
```

- Main function to run the methods and pass the appropriate parameters:

```python
def main():
    final_csv_file, tmp_dir = unzip_file()
    db_processing(final_csv_file, tmp_dir)

if __name__ == '__main__':
    main()
```
