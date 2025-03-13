Timestamp column does not exist

Erroneous code example:
```
IMPORT ecommerce-data.Customer TIMESTAMP cid;
```

Assure that the timestamp column exists. Example:
```
IMPORT ecommerce-data.Customer TIMESTAMP customerid;
```
