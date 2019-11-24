# Download these packages from your Python IDE if you don't have them or if the IDE shows error with them
import csv
import sys
import mysql.connector

db_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="demo"
)

db_cursor = db_conn.cursor()


csv_file_in = '/home/shakir/Desktop/data-from-Redshift/factsales.csv'

with open(csv_file_in, newline='') as csv_file:
    reader = csv.DictReader(csv_file)
    print('Processing . . .')
    try:
        for row in reader:
            sql = "INSERT INTO FactSales (NULL,ProductKey,OrderDateKey,DueDateKey,ShipDateKey,CustomerKey,CurrencyKey,SalesTerritoryKey,SalesOrderNumber,SalesOrderLineNumber,UCOL,RevisionNumber,OrderQuantity,UnitPrice,ExtendedAmount,UnitPriceDiscountPct,DiscountAmount,ProductStandardCost,TotalProductCost,SalesAmount,TaxAmt,Freight,CarrierTrackingNumber,CustomerPONumber,OrderDate,DueDate,ShipDate) " \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            # val = (row['col1'], row['col2'], row['col3'])

            db_cursor.executemany(sql, row)

            sys.stdout.write('Processing \r    ')
            sys.stdout.flush()

    except mysql.connector.Error as err:
            print("SQL Error in INSERT segment:\n", "{}".format(err), "\n")
# Close DB Connection
db_conn.commit()
db_conn.close
db_cursor.close()