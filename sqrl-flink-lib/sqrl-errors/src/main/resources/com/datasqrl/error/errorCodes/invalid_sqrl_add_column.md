Adding a column to an existing table requires that the table is defined immediately
prior to the column definition. 
Adding multiple columns in a row is supported.
Adding columns to imported or created tables is not supported.

Example definition of an additional column on a previously defined `Customer` table:
```
Customers.full_name := CONCAT(first_name, last_name);
```