# Download these packages from your Python IDE if you don't have them or if the IDE shows error with them
import csv
import sys
import mysql.connector

# connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="root", # Put your MySQL username
    passwd="root", # Put your MySQL Password
    database="db" # Put your database name
)

db_cursor = db_conn.cursor()

# Put the complete path, Starting from C: drive, down to your CSV file extension
csv_file_in = 'C:/put/here/the/complete/path/to/your/csv/file/filename.csv'

# Now we will read the csv file
with open(csv_file_in, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        print('Processing . . .')

        # Insert to DB
        for row in reader:
            try:
                # CHANGE TABLENAME below with your actual table name
                # CHANGE col1, col2 below with your actual columns names - add more if you want
                # MATCH the %s characters below with the number of your columns.
                # If you have 3 columns, then put 3 %s characters and so on.
                sql = "INSERT INTO TABLENAME (col1, col2, col3) " \
                      "VALUES (%s, %s, %s)"

                # CHANGE col1, col2 with your CSV file's header names
                val = (row['col1'], row['col2'], row['col3'])

                db_cursor.execute(sql, val)

                sys.stdout.write('Processing \r    ')
                sys.stdout.flush()

            except mysql.connector.Error as err:
                print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")
# Close DB Connection
db_conn.commit()
db_conn.close
db_cursor.close()