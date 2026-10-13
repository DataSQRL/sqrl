A hint argument contains invalid characters.

Hint arguments may only contain names, numbers, and spaces, separated by commas. Column names may be wrapped in backticks, but must not be quoted or qualified with a table name or alias.

Erroneous code example:
```sql
/*+ primary_key(c.customerid) */
CustomerTotals := SELECT c.customerid, SUM(o.amount) AS total
                  FROM Customer c JOIN Orders o ON c.customerid = o.customerid
                  GROUP BY c.customerid;
```

Reference the column by its name in the query result:
```sql
/*+ primary_key(customerid) */
CustomerTotals := SELECT c.customerid, SUM(o.amount) AS total
                  FROM Customer c JOIN Orders o ON c.customerid = o.customerid
                  GROUP BY c.customerid;
```
