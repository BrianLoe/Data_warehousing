#!/bin/sh

## Write your code here to load the data from sales_data table in Mysql server to a sales.csv.
## Select the data which is not more than 4 hours old from the current time.
echo "Extracting data"
mysql --host=127.0.0.1 --port=3306 --user=root --password= -e "use sales;
select 'rowid','product_id','customer_id','price','quantity','timestamp'
union all
select rowid,product_id,customer_id,price,quantity,timestamp
from sales_data where timestamp>=NOW()-INTERVAL 4 HOUR into OUTFILE '/home/project/sales.csv' 
FIELDS TERMINATED BY ',' 
ESCAPED BY '';"

export PGPASSWORD=;
echo "Loading data to staging warehouse"
psql --username=postgres --host=localhost --dbname=sales_new -c "\COPY sales_data(rowid,product_id,customer_id,price,quantity,timestamp) FROM '/home/project/sales.csv' delimiter ',' csv header;" 

## Delete sales.csv present in location /home/project
echo "Removing sales.csv"
rm -f /home/project/sales.csv

## Write your code here to load the DimDate table from the data present in sales_data table
echo "Transforming and loading data into dimdate"
psql --username=postgres --host=localhost --dbname=sales_new -c  "INSERT INTO dimdate(dateid, day, month, year) 
select rowid, EXTRACT(Day FROM timestamp), to_char(timestamp, 'MON'), extract(year from timestamp) from sales_data;"

## Write your code here to load the FactSales table from the data present in sales_data table
echo "Transforming and loading data into factsales"
psql --username=postgres --host=localhost --dbname=sales_new -c  "INSERT INTO factsales(rowid, product_id, custome_id, price, total_price)
SELECT rowid, product_id, customer_id, price, price*quantity from sales_data;"

## Write your code here to export DimDate table to a csv
echo "Exporting dimdate"
psql --username=postgres --host=localhost --dbname=sales_new -c "\COPY dimdate TO '/home/project/dimdate.csv' DELIMITER ',' CSV HEADER;" 

## Write your code here to export FactSales table to a csv
echo "Exporting factsales"
psql --username=postgres --host=localhost --dbname=sales_new -c "\COPY factsales TO '/home/project/factsales.csv' DELIMITER ',' CSV HEADER;"

