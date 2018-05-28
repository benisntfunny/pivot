# Pivot (Oracle)
Pivots unlimited number of column rows into a columns in specified materialized views.

### To Use
Run project or create as jar with an associated properties file for the rows you want to pivot. 

``` sh
java -jar pivot.jar TXN_SHIPPED_ITEM.properties
```

### Known Issues
Errors aren't handled at all. If the files aren't located in the right spot, something is wrong in your database, etc you'll just fail.

#### SAMPLE .properties file
``` .properties
# For connecting to the database
jdbcUrl=jdbc:oracle:thin:@//jdbcurl:portnumber/orcl
jdbcUser=USER
jdbcPassword=PASSWORD

# query to retrieve columns for pivoting
columnQuery = SELECT DISTINCT(ITEM_CD) FROM TXN WHERE ORDER_STATUS = 'Shipped'

# query used to make the pivot excluding the IN statement
pivotQuery = SELECT * FROM ( SELECT CUSTOMERKEY, ITEM_CD FROM TXN WHERE ORDER_STATUS = 'Shipped' ) PIVOT ( COUNT(ITEM_CD) FOR ITEM_CD IN (

# One or more materialized views that will house the pivot data
mvName = MV_TXN_SHIP_ITEM


# Code Table where column names will be stored
codeTableName=CODE_TXN_SHIP_ITEM
```