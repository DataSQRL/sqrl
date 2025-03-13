Ordinals not supported in group by or order by statements.

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT discount 
            FROM @.entries 
            ORDER BY 1;
_____________________^
```

Use the column name instead of an ordinal. Column aliases are supported. Example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT discount 
            FROM @.entries 
            ORDER BY discount;
```
