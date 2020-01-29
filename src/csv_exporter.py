"""
Python script to write MySQL table to a CSV file
"""
import csv
import mysql.connector

# connect to MySQL
db_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",  # not a pswd
    database="dataloader"
)
db_cursor = db_conn.cursor()
table = 'TRANSACTIONS'
sql = "SELECT * FROM %s;" % table
db_cursor.execute(sql)

titles_row = ['id', 'street', 'city', 'zip', 'state', 'beds', 'baths',
              'sq_ft', 'type', 'sale_date', 'price', 'latitude', 'longitude']

# Export destination
with open('/home/shakir/data/output.csv', 'w') as file:
    writer = csv.writer(file)
    # Write titles as first row
    writer.writerow(titles_row)

    for row in db_cursor.fetchall():
        writer.writerow(row)

print("Table exported successfully.")

db_conn.commit()
db_conn.close
db_cursor.close()
